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
)

problems = []
for f in sorted(pathlib.Path(".").rglob("*.go")):
    if ".git" in f.parts:
        continue
    text = f.read_text()
    for imp in re.findall(r'^\s*(?:\w+\s+)?"([^"]+)"', text, re.M):
        if imp.startswith(FORBIDDEN_PREFIXES):
            problems.append(f"{f}: forbidden import {imp}")
        if "." in imp.split("/")[0] and not imp.startswith(ALLOWED_EXTERNAL):
            problems.append(f"{f}: unexpected external import {imp}")
    if re.search(r"(?m)^func init\(\)", text):
        problems.append(f"{f}: init() is not allowed")
if problems:
    print("\n".join(problems), file=sys.stderr)
    sys.exit(1)
print("import guards passed")
PY
