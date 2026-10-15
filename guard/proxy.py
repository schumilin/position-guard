"""代理：默认读取 macOS 系统代理（Clash 等软件开「系统代理」后就会写到这里）。"""
from __future__ import annotations

import urllib.request


def system_proxy() -> str | None:
    try:
        proxies = urllib.request.getproxies()
    except Exception:
        return None
    for key in ("https", "http"):
        url = proxies.get(key)
        if not url:
            continue
        if "://" not in url:
            url = "http://" + url
        if url.startswith("http://"):
            return url.rstrip("/")
    return None


def resolve(mode: str, manual_url: str) -> str | None:
    if mode == "off":
        return None
    if mode == "manual":
        return manual_url.rstrip("/") or None
    return system_proxy()
