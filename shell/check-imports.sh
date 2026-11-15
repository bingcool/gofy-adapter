#!/usr/bin/env bash
# Guard the adapter module's import boundaries and the no-init rule.
# 校验适配层模块的 import 边界与禁止 init() 规则。
set -euo pipefail
cd "$(dirname "$0")/.."
python3 - <<'PY'
import pathlib, re, sys

FORBIDDEN_PREFIXES = (
    "github.com/yourorg/",                              # project contract modules
    "github.com/bingcool/gofy-interface-api/adapter",  # moved into this module
    "github.com/bingcool/gofy-interface-api/client/nacos",
)
ALLOWED_EXTERNAL = (
    "github.com/bingcool/gofy",                        # gofy + gofy-interface-*
    "github.com/gin-gonic/gin",
    "github.com/getkin/kin-openapi",
    "github.com/swaggest/swgui",
    "gopkg.in/yaml.v3",
    # adapter/lease/k8slease: the Kubernetes lock lives here precisely so the
    # core module does not have to carry client-go.
    # adapter/lease/k8slease 用的客户端放在这里，核心模块因此不必背 client-go。
    "k8s.io/client-go",
    "k8s.io/api",
    "k8s.io/apimachinery",
)

problems = []
for f in sorted(pathlib.Path(".").rglob("*.go")):
    if ".git" in f.parts:
        continue
    text = f.read_text()
    for imp in re.findall(r'^\s*(?:[\w.]+\s+)*"([^"]+)"', text, re.M):
        if imp.startswith(FORBIDDEN_PREFIXES):
            problems.append(f"{f}: forbidden import {imp}")
        if "." in imp.split("/")[0] and not imp.startswith(ALLOWED_EXTERNAL):
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
