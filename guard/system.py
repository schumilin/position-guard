"""与 macOS 打交道：单实例、防睡眠、开机自启（含崩溃自动重启）。"""
from __future__ import annotations

import fcntl
import json
import os
import plistlib
import subprocess
import sys
from pathlib import Path

from .paths import data_dir

AGENT_LABEL = "com.positionguard.agent"


def acquire_single_instance():
    f = open(data_dir() / "instance.lock", "w")
    try:
        fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return f
    except OSError:
        f.close()
        return None


def write_instance_info(port: int, token: str) -> None:
    path = data_dir() / "instance.json"
    path.write_text(json.dumps({"port": port, "token": token}), "utf-8")
    os.chmod(path, 0o600)


def read_instance_info() -> dict | None:
    try:
        return json.loads((data_dir() / "instance.json").read_text("utf-8"))
    except (OSError, ValueError):
        return None


def keep_awake() -> subprocess.Popen | None:
    """程序活着就阻止系统睡眠（-s 只在接电源时生效，-i 阻止闲置睡眠）。程序退出后 caffeinate 自动结束。"""
    try:
        return subprocess.Popen(["/usr/bin/caffeinate", "-i", "-s", "-w", str(os.getpid())])
    except OSError:
        return None


def is_frozen() -> bool:
    return bool(getattr(sys, "frozen", False))


def launched_at_login() -> bool:
    return os.environ.get("XPC_SERVICE_NAME") == AGENT_LABEL


def agent_path() -> Path:
    return Path.home() / "Library" / "LaunchAgents" / f"{AGENT_LABEL}.plist"


def set_autostart(enabled: bool) -> str:
    """登录后自动启动；程序异常退出时由 launchd 自动拉起，正常点「退出」则不会。"""
    if not is_frozen():
        return "开发模式，不设置开机自启"
    path = agent_path()
    if not enabled:
        if path.exists():
            path.unlink()
        return "已关闭开机自启"
    plist = {
        "Label": AGENT_LABEL,
        "ProgramArguments": [sys.executable],
        "RunAtLoad": True,
        "KeepAlive": {"SuccessfulExit": False},
        "LimitLoadToSessionType": "Aqua",
        "ProcessType": "Interactive",
        "ThrottleInterval": 10,
    }
    data = plistlib.dumps(plist)
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists() or path.read_bytes() != data:
        path.write_bytes(data)
    return "已开启开机自启"
