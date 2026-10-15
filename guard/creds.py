"""API Key 存取：存在 macOS 钥匙串里，钥匙串不可用时退回到只有本人可读的文件。

只支持实盘。钥匙串条目名沿用 v0.1 的 okx-live，升级后已保存的 Key 继续可用。
"""
from __future__ import annotations

import json
import logging
import os

from .paths import data_dir

log = logging.getLogger(__name__)
SERVICE = "PositionGuard"
ACCOUNT = "okx-live"
FIELDS = ("api_key", "secret_key", "passphrase")


def _keyring():
    try:
        from keyring.backends import macOS
        return macOS.Keyring()
    except Exception as e:  # 非 macOS 或钥匙串不可用
        log.warning("keychain unavailable: %s", e)
        return None


def _file():
    return data_dir() / "cred-live.json"


def _dev_file() -> dict | None:
    """开发测试用：PG_CRED_FILE 指向一个 KEY=VALUE 文件。"""
    vals = {}
    try:
        for line in open(os.environ["PG_CRED_FILE"], encoding="utf-8"):
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                vals[k.strip()] = v.strip()
    except OSError:
        return None
    d = {"api_key": vals.get("OKX_API_KEY", ""), "secret_key": vals.get("OKX_SECRET_KEY", ""),
         "passphrase": vals.get("OKX_PASSPHRASE", "")}
    return d if all(d.values()) else None


def load() -> dict | None:
    if os.environ.get("PG_CRED_FILE"):
        return _dev_file()
    raw = None
    kr = _keyring()
    if kr is not None:
        try:
            raw = kr.get_password(SERVICE, ACCOUNT)
        except Exception as e:
            log.warning("keychain read failed: %s", e)
    if raw is None and _file().exists():
        raw = _file().read_text("utf-8")
    if not raw:
        return None
    try:
        d = json.loads(raw)
    except ValueError:
        return None
    return d if all(d.get(k) for k in FIELDS) else None


def save(api_key: str, secret_key: str, passphrase: str) -> str:
    d = {"api_key": api_key.strip(), "secret_key": secret_key.strip(), "passphrase": passphrase.strip()}
    if not all(d.values()):
        raise ValueError("API Key、Secret Key、Passphrase 三项都要填")
    if any(c.isspace() for v in d.values() for c in v):
        raise ValueError("填写的内容里有空格或换行，请检查是否复制完整")
    raw = json.dumps(d)
    kr = _keyring()
    if kr is not None:
        try:
            kr.set_password(SERVICE, ACCOUNT, raw)
            if _file().exists():
                _file().unlink()
            return "keychain"
        except Exception as e:
            log.warning("keychain write failed, fallback to file: %s", e)
    path = _file()
    path.write_text(raw, "utf-8")
    os.chmod(path, 0o600)
    return "file"


def delete() -> None:
    kr = _keyring()
    if kr is not None:
        try:
            kr.delete_password(SERVICE, ACCOUNT)
        except Exception:
            pass
    if _file().exists():
        _file().unlink()
