"""本机控制面板：只监听 127.0.0.1，接口需要随机令牌，防止网页或局域网里的其他设备乱调。"""
from __future__ import annotations

import logging
import subprocess

from aiohttp import web

from . import creds as creds_mod
from . import system
from .config import parse_watch_list, validate_settings
from .engine import Engine
from .paths import data_dir, resource_dir

log = logging.getLogger(__name__)
PORTS = list(range(18765, 18775))


def create_app(engine: Engine, token: str, port_ref: dict) -> web.Application:
    @web.middleware
    async def guard(request: web.Request, handler):
        port = port_ref.get("port")
        if request.headers.get("Host", "") not in (f"127.0.0.1:{port}", f"localhost:{port}"):
            return web.Response(status=403, text="forbidden")
        if request.path.startswith("/api/") and request.headers.get("X-Token") != token:
            return web.json_response({"error": "面板链接已失效，请点菜单栏的「哨兵」图标 →「打开控制面板」"}, status=401)
        try:
            resp = await handler(request)
        except ValueError as e:
            resp = web.json_response({"error": str(e)}, status=400)
        resp.headers["Cache-Control"] = "no-store"
        return resp

    async def body(request: web.Request) -> dict:
        try:
            data = await request.json()
        except Exception:
            raise ValueError("请求格式不对") from None
        if not isinstance(data, dict):
            raise ValueError("请求格式不对")
        return data

    async def index(_request):
        return web.FileResponse(resource_dir() / "static" / "index.html")

    async def state(_request):
        return web.json_response(engine.snapshot())

    async def save_watch(request):
        watch = parse_watch_list((await body(request)).get("watch", []))
        engine.store.update(watch=watch)
        engine.on_watch_changed()
        engine.event("config", "盯盘设置已更新", engine.watch_summary())
        return web.json_response(engine.snapshot())

    async def save_settings(request):
        changes = validate_settings(await body(request))
        cfg = engine.cfg
        need_restart = any(k in changes and changes[k] != getattr(cfg, k) for k in ("proxy_mode", "proxy_url"))
        engine.store.update(**changes)
        if "autostart" in changes:
            engine.event("config", system.set_autostart(changes["autostart"]))
        if "chase_timeout_sec" in changes or "chase_max_pct" in changes:
            engine.event("config", "平仓方式已更新",
                         f"追逐委托 {cfg.chase_timeout_sec:g} 秒未全部成交就改市价；最大追逐距离 {cfg.chase_max_pct:g}%")
        if need_restart:
            await engine.restart()
        return web.json_response(engine.snapshot())

    async def save_creds(request):
        data = await body(request)
        creds = {k: str(data.get(k) or "").strip() for k in ("api_key", "secret_key", "passphrase")}
        if not all(creds.values()):
            raise ValueError("API Key、Secret Key、Passphrase 三项都要填")
        result = await engine.verify_key(creds)
        if not result["ok"]:
            msg = result["fatal"] or "暂时连不上 OKX，没法验证这把 Key。请检查代理软件后再试。"
            return web.json_response({"error": msg, "checks": list(result["checks"].values())}, status=400)
        where = creds_mod.save(**creds)
        label = result["acct"].get("label") or "无备注"
        engine.event("config", f"已保存 API Key（备注：{label}）",
                     "存在 macOS 钥匙串里" if where == "keychain" else "存在本机私有文件里")
        await engine.restart()
        return web.json_response({"ok": True, "checks": list(result["checks"].values())})

    async def delete_creds(_request):
        creds_mod.delete()
        engine.event("config", "已删除本机保存的 API Key")
        await engine.restart()
        return web.json_response(engine.snapshot())

    async def pause(request):
        engine.set_paused(bool((await body(request)).get("paused")))
        return web.json_response(engine.snapshot())

    async def ack(_request):
        engine.ack()
        return web.json_response(engine.snapshot())

    async def test_alert(_request):
        engine.alerter.play()
        engine.alerter.dialog("仓位哨兵：测试报警", "能看到这个弹窗、听到提示音，说明报警功能正常。", critical=False)
        engine.alerter.notify("仓位哨兵", "这是一条测试通知")
        if engine.cfg.feishu_webhook:
            await engine._push_async(engine.cfg.feishu_webhook, "测试推送：收到这条说明手机推送正常")
        return web.json_response({"ok": True})

    async def open_logs(_request):
        subprocess.Popen(["/usr/bin/open", str(data_dir())])
        return web.json_response({"ok": True})

    app = web.Application(middlewares=[guard])
    app.router.add_get("/", index)
    app.router.add_get("/api/state", state)
    app.router.add_post("/api/watch", save_watch)
    app.router.add_post("/api/settings", save_settings)
    app.router.add_post("/api/credentials", save_creds)
    app.router.add_post("/api/credentials/delete", delete_creds)
    app.router.add_post("/api/pause", pause)
    app.router.add_post("/api/ack", ack)
    app.router.add_post("/api/test-alert", test_alert)
    app.router.add_post("/api/open-logs", open_logs)
    return app


async def start_server(engine: Engine, token: str) -> tuple[web.AppRunner, int]:
    port_ref: dict = {}
    runner = web.AppRunner(create_app(engine, token, port_ref), access_log=None)
    await runner.setup()
    for port in PORTS + [0]:
        site = web.TCPSite(runner, "127.0.0.1", port)
        try:
            await site.start()
        except OSError:
            continue
        port_ref["port"] = site._server.sockets[0].getsockname()[1]
        return runner, port_ref["port"]
    raise RuntimeError("找不到可用端口")
