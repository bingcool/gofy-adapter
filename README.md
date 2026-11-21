# gofy-adapter

模块版本：`adapter.Version`（当前 `1.0.0`），供应用在启动日志里与 `gofy.Version` 一起记录。

把 `gofy-interface-api` 的纯契约原语适配到 gofy 框架的桥接层，包含：

- `adapter/gofy/`：把 `endpoint.Descriptor` 绑定到 `support.Router`，提供
  `BindGroup` / `BindAll` / `BindOptions` / `Error` / `DocsModule`；
  `DocsModule` 仅在 `docsui.Config.Enabled` 为 true 时挂载路由，零值配置不会
  发布文档。
- `adapter/lease/k8slease/`：用 Kubernetes `coordination.k8s.io/v1` 的 Lease 对象
  实现 `gofy/support/lease.Locker`，见下节；
- `client/nacos/`：基于 `gofy/support/nacos` 的服务发现 Resolver，接入
  `gofy-interface-api/client.Resolver`。

## 为什么单独成库

`gofy-interface-api` 是框架无关的纯契约库（`go.mod` 不含 gofy / gin），
本模块承担全部 gofy 耦合，两者互不依赖：

```
gofy-interface-api  <---- require ----  gofy-adapter  ---- require ---->  gofy
        ^                                      ^                                  ^
        |                                      |                                  |
   项目契约模块                          业务服务 demo-service  -------------------+

adapter/gofy        -> gofy, gin, gofy-interface-api
adapter/lease/k8slease -> gofy/support/lease, k8s.io/api, k8s.io/client-go, k8s.io/apimachinery
client/nacos        -> gofy/support/nacos, gofy-interface-api
```

`gofy` 与 `gofy-interface-api` 之间**没有依赖边**，因此 api 库可以独立打 tag、
独立被非 gofy 框架的项目消费。

## Kubernetes Lease 适配器

`adapter/lease/k8slease` 用 `coordination.k8s.io/v1` 的 Lease 对象做分布式锁，也就是
Kubernetes 自身选举用的那套对象。它放在本模块而不是 `gofy`，是因为 `client-go` 会把
一整套 API machinery 拖进 `go.mod`，用 Redis 加锁的项目不该为它买单。

```go
locker, err := k8slease.NewForConfig(restCfg, k8slease.Config{
    Namespace: "prod",
    // 必须每个进程唯一：一个 Pod 只跑一个应用进程时用 Pod 名即可，
    // 否则用 Pod UID + 进程/实例标识（如 podName + "/" + pid），
    // 两个进程共用一个 Identity 会互相"恢复续租"。
    Identity:  podName + "/" + strconv.Itoa(os.Getpid()),
})
held, err := locker.Acquire(ctx, "settle-reconcile", 30*time.Second)
if errors.Is(err, lease.ErrHeld) {
    // 别人持有，稍后重试
}
defer held.Release(ctx)
```

**内置超时与 TTL 上限**：`Config.RequestTimeout` 限制每次 API 交互（`Acquire`、
`Renew`、`Release`）的时长，也是 `Close` 等待在途操作的上限，零值取默认 10 秒，
负值在构造时被拒绝。调用方的 `ctx`
通常没有 deadline，没有这条预算时，被黑洞的 API server 会让租约操作永久挂起；
`NewForConfig` 同时把它写进 `rest.Config.Timeout`（`rest.CopyConfig` 复制后再改，
不改动调用方配置），形成传输层兜底，调用方显式设置的 `rest.Config.Timeout` 优先。
TTL 上限是 `LeaseDurationSeconds` 的 int32 范围（约 68 年）：更长的值会在换算
秒数时溢出并悄悄退化成 1 秒，因此 `Acquire` 在入口用 `lease.ErrInvalidTTL` 拒绝。

**Release 与 Close**：`Locker` 按 key 保留进程内占用（防止同一进程的两个 goroutine
同时"恢复"同一租约），因此调用方必须 `Release` 自己取得的每个句柄；丢弃句柄会让该
key 在本进程内一直返回 `ErrHeld`。`Locker.Close()` 清空占用表并拒绝后续 `Acquire`
（返回 `k8slease.ErrClosed`），Close 开始时仍在途的 `Acquire` 同样以 `ErrClosed`
失败而不交付句柄。Close 最多等待 `RequestTimeout` 让在途 API 操作结束，等待用尽时
返回 `k8slease.ErrCloseTimeout`（Locker 仍然已关闭，之后再调用 Close 可继续等待）；
Close 之前取得的句柄仍可 `Release`，用于给停机流程兜底。同一句柄上的 `Renew` 与
`Release` 由互斥锁串行化，并发续期不会因 resourceVersion 冲突误报 `ErrLeaseLost`。

**只编译一个 API 组**：`NewForConfig` 用 `kubernetes/typed/coordination/v1` 的类型化
客户端，而不是完整 `Clientset`，因此 `go.sum` 里不会出现所有内置 API 类型。

**RBAC 最小权限**（Lease 名即 key 经 DNS-1123 校验后的形式，不做字符替换）：

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  namespace: prod
  name: gofy-lease
rules:
  - apiGroups: ["coordination.k8s.io"]
    resources: ["leases"]
    verbs: ["get", "create", "update"]   # 不需要 list/watch/delete
```

**三条硬约束**：

1. **Identity 必须每个进程唯一**。只有"一个 Pod 跑一个应用进程"时才能直接用 Pod 名；
   一个 Pod 内多进程（sidecar、多实例）必须再加进程/实例标识，例如 `podName + "/" + pid`，
   跨重启稳定时可用 Pod UID。两个进程用同一 Identity 会互相"恢复续租"，都以为自己持有锁。
2. **不要删除 Lease 对象**。fencing token 取 `spec.leaseTransitions`，删对象会让它
   归零，而被保护的资源可能仍在用旧 token；只需要 `get/create/update` 权限，RBAC 里
   不给 `delete` 就是最直接的保险。
3. **同一 `(Identity, key)` 在进程内是单飞的**。同名 Pod 重启后重新获取自己持有的
   租约时沿用原 token（它自己还在途的写入可能仍以该 token 做 fencing），但这不是
   "同一进程可以并发持锁"：`Locker` 内部按 key 记录活跃获取，第二次 `Acquire` 返回
   `ErrHeld`，`Release` 或 `Renew` 发现被接管后释放该记录。同一进程内需要并发临界区
   时，请用不同的 key。

## 本地联调

`go.mod` 的两条 `replace` 要求三个仓库并排 checkout：

```
wwwroot/
├── gofy/
├── gofy-interface-api/
└── gofy-adapter/   # 本仓库
```

```bash
make check    # fmt-check + vet + test-race + import guards
```

发布时删除两条 replace，改为固定 `gofy` 与 `gofy-interface-api` 的已发布版本；
发布顺序：`gofy` 与 `gofy-interface-api`（可并行）→ 本库 → 项目契约模块 → 业务服务。

## 约束

`adapter/gofy` 允许依赖 gofy / gin / api 库；`client/nacos` 允许依赖
`gofy/support/nacos` 与 api 库；`adapter/lease/k8slease` 只允许依赖
`gofy/support/lease` 与 `k8s.io/*`，不依赖 gin、api 库或 `gofy` 主包。全库禁止 import
项目契约模块、禁止 `func init()`。规则由 `shell/check-imports.sh` 强制。
