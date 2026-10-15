"""配置：规则 + 运行设置，存成 JSON。"""
from __future__ import annotations

import json
import os
import re
import threading
import uuid
from dataclasses import asdict, dataclass, field
from pathlib import Path

from .paths import data_dir

ACTIONS = {"close_all": "市价全平", "close_pct": "按比例平仓", "alert": "只报警"}
DIRECTIONS = {"adverse": "不利", "favorable": "有利"}
ENVS = {"demo": "模拟盘", "live": "实盘"}
PROXY_MODES = ("auto", "manual", "off")
MAX_WINDOW_MIN = 360
INST_RE = re.compile(r"^[A-Z0-9]+-[A-Z0-9]+-SWAP$")


@dataclass
class Rule:
    id: str
    window_min: float
    direction: str
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
            raise ValueError("涨跌幅阈值要大于 0% 且小于 100%")
        if direction not in DIRECTIONS:
            raise ValueError("方向只能是「不利」或「有利」")
        if action not in ACTIONS:
            raise ValueError("动作只能是市价全平、按比例平仓或只报警")
        if action == "close_pct" and not 1 <= close_pct <= 99:
            raise ValueError("平仓比例要在 1% 到 99% 之间（要全平请选「市价全平」）")
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


def parse_overrides(d) -> dict[str, list[Rule]]:
    if not isinstance(d, dict):
        raise ValueError("单币规则格式不对")
    out = {}
    for inst_id, items in d.items():
        if not INST_RE.match(inst_id):
            raise ValueError(f"合约名称不对：{inst_id}（应该像 BTC-USDT-SWAP 这样）")
        out[inst_id] = parse_rules(items)
    return out


@dataclass
class Config:
    env: str = "live"
    proxy_mode: str = "auto"
    proxy_url: str = ""
    paused: bool = False
    default_rules: list[Rule] = field(default_factory=list)
    overrides: dict[str, list[Rule]] = field(default_factory=dict)
    feishu_webhook: str = ""
    autostart: bool = True
    sample_interval_sec: float = 1.0
    stale_sec: float = 30.0

    def rules_for(self, inst_id: str) -> list[Rule]:
        rules = self.overrides.get(inst_id, self.default_rules)
        return [r for r in rules if r.enabled]

    def all_rules(self) -> list[Rule]:
        return list(self.default_rules) + [r for rs in self.overrides.values() for r in rs]

    def max_window_min(self) -> float:
        return max((r.window_min for r in self.all_rules()), default=0.0)

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "Config":
        cfg = cls()
        if d.get("env") in ENVS:
            cfg.env = d["env"]
        if d.get("proxy_mode") in PROXY_MODES:
            cfg.proxy_mode = d["proxy_mode"]
        cfg.proxy_url = str(d.get("proxy_url") or "")
        cfg.paused = bool(d.get("paused", False))
        cfg.feishu_webhook = str(d.get("feishu_webhook") or "")
        cfg.autostart = bool(d.get("autostart", True))
        for key in ("sample_interval_sec", "stale_sec"):
            try:
                setattr(cfg, key, float(d.get(key, getattr(cfg, key))))
            except (TypeError, ValueError):
                pass
        try:
            cfg.default_rules = parse_rules(d.get("default_rules") or [])
        except ValueError:
            cfg.default_rules = []
        try:
            cfg.overrides = parse_overrides(d.get("overrides") or {})
        except ValueError:
            cfg.overrides = {}
        return cfg


def validate_settings(d: dict) -> dict:
    """面板「设置」页提交的字段，校验后返回可直接写入 Config 的部分。"""
    out = {}
    if "env" in d:
        if d["env"] not in ENVS:
            raise ValueError("环境只能是模拟盘或实盘")
        out["env"] = d["env"]
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
