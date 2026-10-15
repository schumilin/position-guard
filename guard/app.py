"""入口：菜单栏图标在主线程，监控引擎和控制面板在后台线程的 asyncio 里。"""
from __future__ import annotations

import argparse
import asyncio
import logging
import logging.handlers
import secrets
import sys
import threading
import webbrowser

from . import VERSION, system
from .alerts import Alerter
from .config import ENVS, ConfigStore
from .engine import Engine
from .paths import APP_TITLE, data_dir
from .web import start_server

log = logging.getLogger("guard")
ICONS = {"ok": "🟢", "paused": "🟡", "down": "🔴", "error": "🔴", "setup": "⚪", "starting": "⏳"}


def setup_logging() -> None:
    handler = logging.handlers.RotatingFileHandler(data_dir() / "app.log", maxBytes=2_000_000, backupCount=3,
                                                   encoding="utf-8")
    handlers: list[logging.Handler] = [handler]
    if not system.is_frozen():
        handlers.append(logging.StreamHandler())
    logging.basicConfig(level=logging.INFO, handlers=handlers,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    logging.getLogger("aiohttp").setLevel(logging.WARNING)


class Runtime:
    """在后台线程里跑 asyncio：引擎 + 控制面板。"""

    def __init__(self) -> None:
        self.store = ConfigStore()
        self.loop = asyncio.new_event_loop()
        self.alerter = Alerter(on_dialog_ack=lambda: self.call(self.engine.ack))
        self.engine: Engine | None = None
        self.token = secrets.token_urlsafe(24)
        self.port: int | None = None
        self.ready = threading.Event()

    def start_thread(self) -> None:
        threading.Thread(target=self.run_forever, name="engine", daemon=True).start()
        self.ready.wait(30)

    def run_forever(self) -> None:
        asyncio.set_event_loop(self.loop)
        self.loop.run_until_complete(self._main())
        self.loop.run_forever()

    async def _main(self) -> None:
        self.engine = Engine(self.store, self.alerter)
        self.engine.event("info", f"程序启动（版本 {VERSION}）")
        _, self.port = await start_server(self.engine, self.token)
        system.write_instance_info(self.port, self.token)
        await self.engine.start()
        self.ready.set()

    def url(self) -> str:
        return f"http://127.0.0.1:{self.port}/#t={self.token}"

    def call(self, fn) -> None:
        self.loop.call_soon_threadsafe(fn)


def run_menubar(rt: Runtime) -> None:
    import rumps

    class MenuApp(rumps.App):
        def __init__(self) -> None:
            super().__init__(APP_TITLE, title="⏳ 哨兵", quit_button=None)
            self.status_item = rumps.MenuItem("正在启动…")
            self.env_item = rumps.MenuItem("")
            self.pause_item = rumps.MenuItem("暂停自动平仓", callback=self.toggle_pause)
            self.ack_item = rumps.MenuItem("停止响铃")
            self.menu = [
                self.status_item, self.env_item, None,
                rumps.MenuItem("打开控制面板", callback=self.open_panel),
                self.pause_item, self.ack_item, None,
                rumps.MenuItem("退出仓位哨兵", callback=self.quit_app),
            ]
            self.timer = rumps.Timer(self.refresh, 1)
            self.timer.start()

        def refresh(self, _sender) -> None:
            engine = rt.engine
            if engine is None:
                return
            try:  # 引擎在另一个线程里改状态，这里只读快照，读失败下一秒再来
                self.title = f"{ICONS.get(engine.status, '⏳')} 哨兵"
                self.status_item.title = engine.status_text[:60]
                self.env_item.title = f"环境：{ENVS[engine.cfg.env]}"
                self.pause_item.title = "恢复自动平仓" if engine.cfg.paused else "暂停自动平仓"
                ringing = engine.alerter.sounding or any(not a["acked"] for a in list(engine.alarms.values()))
                self.ack_item.set_callback(self.ack if ringing else None)
            except Exception:
                log.exception("menubar refresh failed")

        def open_panel(self, _sender) -> None:
            webbrowser.open(rt.url())

        def toggle_pause(self, _sender) -> None:
            engine = rt.engine
            rt.call(lambda: engine.set_paused(not engine.cfg.paused))

        def ack(self, _sender) -> None:
            rt.call(rt.engine.ack)

        def quit_app(self, _sender) -> None:
            if rumps.alert("退出仓位哨兵？", "退出后不再监控仓位，也不会自动平仓。", ok="退出", cancel="取消") == 1:
                rumps.quit_application()

    MenuApp().run()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--headless", action="store_true", help="不显示菜单栏图标（开发调试用）")
    parser.add_argument("--no-browser", action="store_true", help="启动时不自动打开控制面板")
    args, _ = parser.parse_known_args()

    setup_logging()
    lock = system.acquire_single_instance()
    if lock is None:  # 已经在运行了：打开它的面板就退出
        info = system.read_instance_info()
        if info:
            webbrowser.open(f"http://127.0.0.1:{info['port']}/#t={info['token']}")
        sys.exit(0)

    system.keep_awake()
    rt = Runtime()
    if rt.store.cfg.autostart:
        system.set_autostart(True)
    if args.headless:
        threading.Thread(target=lambda: (rt.ready.wait(30), print("控制面板：", rt.url(), flush=True)),
                         daemon=True).start()
        rt.run_forever()
        return
    rt.start_thread()
    if not args.no_browser and not system.launched_at_login():
        webbrowser.open(rt.url())
    run_menubar(rt)


if __name__ == "__main__":
    main()
