# gofy-adapter

把 `gofy-interface-api` 的纯契约原语适配到 gofy 框架的桥接层，包含：

- `adapter/gofy/`：把 `endpoint.Descriptor` 绑定到 `support.Router`，提供
  `BindGroup` / `BindAll` / `BindOptions` / `Error` / `DocsModule`；
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
    Identity:  podName, // 必须每个进程唯一
})
held, err := locker.Acquire(ctx, "settle-reconcile", 30*time.Second)
if errors.Is(err, lease.ErrHeld) {
    // 别人持有，稍后重试
}
defer held.Release(ctx)
```

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

1. **Identity 必须每个进程唯一**（Pod 名即可）。两个进程用同一 Identity 会互相"恢复
   续租"，都以为自己持有锁。
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
