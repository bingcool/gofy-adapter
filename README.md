# gofy-adapter

把 `gofy-interface-api` 的纯契约原语适配到 gofy 框架的桥接层，包含：

- `adapter/gofy/`：把 `endpoint.Descriptor` 绑定到 `support.Router`，提供
  `BindGroup` / `BindAll` / `BindOptions` / `Error` / `DocsModule`；
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
```

`gofy` 与 `gofy-interface-api` 之间**没有依赖边**，因此 api 库可以独立打 tag、
独立被非 gofy 框架的项目消费。

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
`gofy/support/nacos` 与 api 库；全库禁止 import 项目契约模块、禁止 `func init()`。
规则由 `shell/check-imports.sh` 强制。
