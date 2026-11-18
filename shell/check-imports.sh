#!/usr/bin/env bash
# Guard the adapter module's import boundaries and the no-init rule.
# 校验适配层模块的 import 边界与禁止 init() 规则。
set -euo pipefail
cd "$(dirname "$0")/.."
python3 - <<'PY'
import pathlib, re, sys

FORBIDDEN_PREFIXES = (
    # The sibling example projects really use these module paths (see
    # examples/*/go.mod); the adapter is generic and must not depend on a
    # consumer's app code or contracts.
    # 同仓库的示例项目确实使用这两个模块路径（见 examples/*/go.mod）；适配层是
    # 通用模块，不得依赖使用方的应用代码或契约。
    "github.com/yourorg/demo-service",
    "github.com/yourorg/interface-service",
    "github.com/bingcool/gofy-interface-api/adapter",  # moved into this module
    "github.com/bingcool/gofy-interface-api/client/nacos",
)
ALLOWED_EXTERNAL = (
    "github.com/bingcool/gofy",                        # gofy + gofy-interface-*
    "github.com/gin-gonic/gin",
    "github.com/getkin/kin-openapi",
    "github.com/swaggest/swgui",
    "gopkg.in/yaml.v3",
)
# Only the Kubernetes lease adapter may pull in k8s.io. Scoping the allowlist
# to that directory keeps client-go from leaking into the rest of the module.
# 只有 Kubernetes 租约适配器可以引入 k8s.io；白名单限定在该目录，client-go 不会
# 渗到模块的其他部分。
ALLOWED_K8S_EXTERNAL = (
    "k8s.io/client-go",
    "k8s.io/api",
    "k8s.io/apimachinery",
)
K8S_LEASE_DIR = "adapter/lease/k8slease/"

problems = []
for f in sorted(pathlib.Path(".").rglob("*.go")):
    if ".git" in f.parts:
        continue
    text = f.read_text()
    allowed = ALLOWED_EXTERNAL
    if f.as_posix().startswith(K8S_LEASE_DIR):
        allowed = ALLOWED_EXTERNAL + ALLOWED_K8S_EXTERNAL
    for imp in re.findall(r'^\s*(?:[\w.]+\s+)*"([^"]+)"', text, re.M):
        if imp.startswith(FORBIDDEN_PREFIXES):
            problems.append(f"{f}: forbidden import {imp}")
        if "." in imp.split("/")[0] and not imp.startswith(allowed):
            problems.append(f"{f}: unexpected external import {imp}")
    if re.search(r"(?m)^func init\(\)", text):
        problems.append(f"{f}: init() is not allowed")

# The Kubernetes lock adapter must stay usable from a worker-only process, so it
# may not reach for gin or the gofy root package; support/lease and k8s.io are
# all it needs.
# Kubernetes 锁适配器必须能在纯 worker 进程里使用，因此不得依赖 gin 或 gofy 根包：
# 它只需要 support/lease 与 k8s.io。
for f in sorted(pathlib.Path("adapter/lease/k8slease").rglob("*.go")):
    text = f.read_text()
    for imp in re.findall(r'^\s*(?:[\w.]+\s+)*"([^"]+)"', text, re.M):
        if imp in ("github.com/gin-gonic/gin", "github.com/bingcool/gofy"):
            problems.append(f"{f}: the lease adapter must not import {imp}")
if problems:
    print("\n".join(problems), file=sys.stderr)
    sys.exit(1)
print("import guards passed")
PY
