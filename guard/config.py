"""配置：盯盘列表（标的 + 价格条件 + 规则）和运行设置，存成 JSON。"""
from __future__ import annotations

import json
import os
import re
import threading
import uuid
from dataclasses import asdict, dataclass, field
from pathlib import Path

from .paths import data_dir

ACTIONS = {"close_all": "全平", "close_pct": "按比例平仓", "alert": "只报警"}
DIRECTIONS = {"up": "涨", "down": "跌"}
GATE_SIDES = {"above": "高于", "below": "低于"}
PROXY_MODES = ("auto", "manual", "off")
MAX_WINDOW_MIN = 360
INST_RE = re.compile(r"^[A-Z0-9]+-[A-Z0-9]+-SWAP$")


@dataclass
class Rule:
    id: str
    window_min: float
    direction: str        # up / down：标记价涨 / 跌
    threshold_pct: float
    action: str
    close_pct: float = 50.0
    enabled: bool = True

    @classmethod
    def parse(cls, d: dict) -> "Rule":
        try:
            window = float(d["window_min"])
            threshold = float(d["threshold_pct"])
            close_pct = float(d.get("close_pct") or 50)
        except (KeyError, TypeError, ValueError):
            raise ValueError("规则里有数字没填，或者格式不对") from None
        direction = d.get("direction")
        action = d.get("action")
        if not 0 < window <= MAX_WINDOW_MIN:
            raise ValueError(f"时间窗口要在 0 到 {MAX_WINDOW_MIN} 分钟之间")
        if window * 60 < 2:
            raise ValueError("时间窗口至少 2 秒（0.04 分钟）")
        if not 0 < threshold < 100:
            raise ValueError("涨跌幅要大于 0% 且小于 100%")
        if direction not in DIRECTIONS:
            raise ValueError("方向只能是「涨」或「跌」")
        if action not in ACTIONS:
            raise ValueError("动作只能是全平、按比例平仓或只报警")
        if action == "close_pct" and not 1 <= close_pct <= 99:
            raise ValueError("平仓比例要在 1% 到 99% 之间（要全平请选「全平」）")
        rid = str(d.get("id") or uuid.uuid4().hex[:8])
        return cls(rid, window, direction, threshold, action, close_pct, bool(d.get("enabled", True)))


def parse_rules(items) -> list[Rule]:
    if not isinstance(items, list):
        raise ValueError("规则格式不对")
    rules = [Rule.parse(x) for x in items]
    ids = [r.id for r in rules]
    if len(set(ids)) != len(ids):
        raise ValueError("规则编号重复")
    return rules


@dataclass
class Watch:
    """一个盯盘标的：先过价格条件，再看规则。"""
    inst_id: str
    gate_side: str        # above / below
    gate_px: float
    rules: list[Rule] = field(default_factory=list)
    enabled: bool = True

    @property
    def coin(self) -> str:
        return self.inst_id.split("-")[0]

    def gate_ok(self, px: float) -> bool:
        return px >= self.gate_px if self.gate_side == "above" else px <= self.gate_px

    @classmethod
    def parse(cls, d: dict) -> "Watch":
        inst_id = str(d.get("inst_id") or "").strip().upper()
        if not INST_RE.match(inst_id):
            raise ValueError(f"合约名称不对：{inst_id or '空'}（应该像 BTC-USDT-SWAP 这样）")
        coin = inst_id.split("-")[0]
        if d.get("gate_side") not in GATE_SIDES:
            raise ValueError(f"{coin}：请先设置价格条件（高于还是低于）")
        try:
            gate_px = float(d.get("gate_px"))
        except (TypeError, ValueError):
            raise ValueError(f"{coin}：请先填写价格条件里的价格") from None
        if not gate_px > 0:
            raise ValueError(f"{coin}：价格条件里的价格要大于 0")
        try:
            rules = parse_rules(d.get("rules") or [])
        except ValueError as e:
            raise ValueError(f"{coin}：{e}") from None
        return cls(inst_id, d["gate_side"], gate_px, rules, bool(d.get("enabled", True)))


def parse_watch_list(items) -> list[Watch]:
    if not isinstance(items, list):
        raise ValueError("盯盘列表格式不对")
    out = [Watch.parse(x) for x in items]
    insts = [w.inst_id for w in out]
    if len(set(insts)) != len(insts):
        raise ValueError("同一个合约只能添加一次")
    return out


@dataclass
class Config:
    proxy_mode: str = "auto"
    proxy_url: str = ""
    paused: bool = False
    watch: list[Watch] = field(default_factory=list)
    chase_timeout_sec: float = 30.0   # 追逐委托多久没全部成交就改市价
    chase_max_pct: float = 1.0        # 最大追逐距离（相对首次挂单价的百分比），超过 OKX 会撤单
    feishu_webhook: str = ""
    autostart: bool = True
    sample_interval_sec: float = 1.0
    stale_sec: float = 30.0

    def watch_for(self, inst_id: str) -> Watch | None:
        return next((w for w in self.watch if w.inst_id == inst_id), None)

    def max_window_min(self) -> float:
        return max((r.window_min for w in self.watch for r in w.rules), default=0.0)

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "Config":
        cfg = cls()
        if d.get("proxy_mode") in PROXY_MODES:
            cfg.proxy_mode = d["proxy_mode"]
        cfg.proxy_url = str(d.get("proxy_url") or "")
        cfg.paused = bool(d.get("paused", False))
        cfg.feishu_webhook = str(d.get("feishu_webhook") or "")
        cfg.autostart = bool(d.get("autostart", True))
        for key in ("sample_interval_sec", "stale_sec", "chase_timeout_sec", "chase_max_pct"):
            try:
                setattr(cfg, key, float(d.get(key, getattr(cfg, key))))
            except (TypeError, ValueError):
                pass
        watch = []
        for item in d.get("watch") or []:  # 坏掉的条目跳过，不影响其他标的
            try:
                watch.append(Watch.parse(item))
            except (ValueError, AttributeError):
                pass
        cfg.watch = watch
        return cfg


def validate_settings(d: dict) -> dict:
    """面板「设置」页提交的字段，校验后返回可直接写入 Config 的部分。"""
    out = {}
    if "proxy_mode" in d:
        if d["proxy_mode"] not in PROXY_MODES:
            raise ValueError("代理模式不对")
        out["proxy_mode"] = d["proxy_mode"]
    if "proxy_url" in d:
        url = str(d["proxy_url"] or "").strip()
        if url and not re.match(r"^http://[\w.\-]+:\d+/?$", url):
            raise ValueError("代理地址要像 http://127.0.0.1:7890 这样（只支持 http 代理）")
        out["proxy_url"] = url
    if out.get("proxy_mode", d.get("proxy_mode")) == "manual" and not out.get("proxy_url", d.get("proxy_url")):
        raise ValueError("选了手动代理，就要填代理地址")
    if "chase_timeout_sec" in d:
        try:
            v = float(d["chase_timeout_sec"])
        except (TypeError, ValueError):
            raise ValueError("超时秒数格式不对") from None
        if not 3 <= v <= 600:
            raise ValueError("追逐委托超时要在 3 到 600 秒之间")
        out["chase_timeout_sec"] = v
    if "chase_max_pct" in d:
        try:
            v = float(d["chase_max_pct"])
        except (TypeError, ValueError):
            raise ValueError("最大追逐距离格式不对") from None
        if not 0.05 <= v <= 20:
            raise ValueError("最大追逐距离要在 0.05% 到 20% 之间")
        out["chase_max_pct"] = v
    if "feishu_webhook" in d:
        url = str(d["feishu_webhook"] or "").strip()
        if url and not url.startswith("https://"):
            raise ValueError("飞书机器人地址应以 https:// 开头")
        out["feishu_webhook"] = url
    if "autostart" in d:
        out["autostart"] = bool(d["autostart"])
    return out


class ConfigStore:
    def __init__(self, path: Path | None = None) -> None:
        self.path = path or data_dir() / "config.json"
        self._lock = threading.Lock()
        self.cfg = self._load()

    def _load(self) -> Config:
        try:
            return Config.from_dict(json.loads(self.path.read_text("utf-8")))
        except (OSError, ValueError):
            return Config()

    def save(self) -> None:
        with self._lock:
            tmp = self.path.with_suffix(".tmp")
            tmp.write_text(json.dumps(self.cfg.to_dict(), ensure_ascii=False, indent=2), "utf-8")
            os.replace(tmp, self.path)

    def update(self, **changes) -> Config:
        for k, v in changes.items():
            setattr(self.cfg, k, v)
        self.save()
        return self.cfg
