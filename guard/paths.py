"""数据目录与资源目录。"""
from __future__ import annotations

import os
import sys
from pathlib import Path

APP_NAME = "PositionGuard"
APP_TITLE = "仓位哨兵"


def data_dir() -> Path:
    """配置、日志、事件记录存放处。开发和测试时可用 PG_DATA_DIR 改到别处。"""
    custom = os.environ.get("PG_DATA_DIR")
    path = Path(custom) if custom else Path.home() / "Library" / "Application Support" / APP_NAME
    path.mkdir(parents=True, exist_ok=True)
    return path


def resource_dir() -> Path:
    """打包后资源在 sys._MEIPASS/guard 下，开发时就在源码目录。"""
    base = getattr(sys, "_MEIPASS", None)
    return Path(base) / "guard" if base else Path(__file__).parent
