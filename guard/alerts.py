"""Mac 弹窗、通知和报警声。"""
from __future__ import annotations

import logging
import os
import subprocess
import threading
import time
from typing import Callable

log = logging.getLogger(__name__)

SOUND_ALARM = "/System/Library/Sounds/Sosumi.aiff"
SOUND_INFO = "/System/Library/Sounds/Glass.aiff"
MAX_DIALOGS = 3
MAX_ALARM_SEC = 30 * 60


class Alerter:
    def __init__(self, on_dialog_ack: Callable[[], None] | None = None) -> None:
        self.enabled = os.environ.get("PG_NO_OS_ALERTS") != "1"
        self.on_dialog_ack = on_dialog_ack
        self._dialogs: list[subprocess.Popen] = []
        self._stop = threading.Event()
        self._sound_thread: threading.Thread | None = None
        self.sounding = False

    def _osascript(self, lines: list[str], *args: str) -> subprocess.Popen | None:
        if not self.enabled:
            return None
        cmd = ["/usr/bin/osascript"]
        for line in lines:
            cmd += ["-e", line]
        try:
            return subprocess.Popen(cmd + list(args), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        except OSError as e:
            log.warning("osascript failed: %s", e)
            return None

    def notify(self, title: str, msg: str) -> None:
        self._osascript(["on run argv", "display notification (item 2 of argv) with title (item 1 of argv)", "end run"],
                        title, msg[:300])

    def play(self, sound: str = SOUND_INFO) -> None:
        if self.enabled:
            try:
                subprocess.Popen(["/usr/bin/afplay", sound], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            except OSError:
                pass

    def dialog(self, title: str, msg: str, critical: bool) -> None:
        self._dialogs = [p for p in self._dialogs if p.poll() is None]
        if len(self._dialogs) >= MAX_DIALOGS:
            return
        kind = "critical" if critical else "informational"
        p = self._osascript([
            "on run argv",
            f'display alert (item 1 of argv) message (item 2 of argv) as {kind} buttons {{"知道了"}} default button 1',
            "end run",
        ], title, msg[:1500])
        if p is None:
            return
        self._dialogs.append(p)
        if critical and self.on_dialog_ack:
            def wait() -> None:
                if p.wait() == 0 and self.on_dialog_ack:  # 用户点了「知道了」
                    self.on_dialog_ack()
            threading.Thread(target=wait, daemon=True).start()

    def start_alarm(self) -> None:
        if not self.enabled:
            return
        self._stop.clear()
        self.sounding = True
        if self._sound_thread and self._sound_thread.is_alive():
            return  # 正在响（或刚被叫停、还没退出）的线程会接着响

        def loop() -> None:
            start = time.time()
            while not self._stop.is_set() and time.time() - start < MAX_ALARM_SEC:
                try:
                    subprocess.run(["/usr/bin/afplay", SOUND_ALARM], timeout=10)
                except Exception:
                    pass
                self._stop.wait(1.2)
            self.sounding = False

        self._sound_thread = threading.Thread(target=loop, daemon=True)
        self._sound_thread.start()

    def stop_alarm(self, close_dialogs: bool = True) -> None:
        self._stop.set()
        self.sounding = False
        if close_dialogs:
            for p in self._dialogs:
                if p.poll() is None:
                    p.terminate()
            self._dialogs = []
