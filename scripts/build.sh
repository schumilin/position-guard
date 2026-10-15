#!/bin/bash
# 打包成 dist/PositionGuard.app 和 dist/PositionGuard.zip
set -euo pipefail
cd "$(dirname "$0")/.."
uv run pytest -q
uv run --with pillow python scripts/make_icon.py
uv run pyinstaller --noconfirm --clean PositionGuard.spec
rm -f dist/PositionGuard.zip
ditto -c -k --keepParent dist/PositionGuard.app dist/PositionGuard.zip
du -sh dist/PositionGuard.app dist/PositionGuard.zip
