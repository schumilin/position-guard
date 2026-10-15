"""规则判断（纯函数，便于测试）。

涨跌幅 = (当前标记价 − 锚点价) ÷ 锚点价，锚点价见 PriceSeries.anchor。
规则「N 分钟涨 / 跌 X%」只在标的的价格条件（高于 / 低于某价）当下满足时才生效，这一层在引擎里判断。
"""
from __future__ import annotations

from dataclasses import dataclass
from decimal import ROUND_DOWN, Decimal

from .config import Rule
from .series import PriceSeries

ACTION_RANK = {"alert": 1, "close_pct": 2, "close_all": 3}


@dataclass
class Evaluation:
    rule: Rule
    target_ts: int      # 锚点时刻 = 现在 − N 分钟
    anchor_ts: int      # 实际取到的采样点时间
    anchor_px: float
    cur_px: float
    change_pct: float   # 标记价涨跌幅，正数 = 涨
    move_pct: float     # 朝本规则方向的幅度，正数 = 往触发方向走
    hit: bool           # 只看涨跌幅是否达到，不含价格条件


def evaluate(rule: Rule, series: PriceSeries, now_ms: int, cur_px: float) -> Evaluation | None:
    target = now_ms - int(round(rule.window_min * 60_000))
    found = series.anchor(target)
    if found is None or cur_px <= 0:
        return None
    anchor_ts, anchor_px = found
    if anchor_px <= 0:
        return None
    change = (cur_px - anchor_px) / anchor_px * 100.0
    move = change if rule.direction == "up" else -change
    return Evaluation(rule, target, anchor_ts, anchor_px, cur_px, change, move, move >= rule.threshold_pct - 1e-9)


def pick(hits: list[Evaluation]) -> Evaluation:
    """同时满足多条规则时，只执行最重的动作：全平 > 按比例平 > 只报警。"""
    return max(hits, key=lambda e: (ACTION_RANK[e.rule.action], e.rule.close_pct if e.rule.action == "close_pct" else 0))


def partial_size(pos_abs: Decimal, pct: float, lot_sz: Decimal, min_sz: Decimal) -> Decimal | None:
    """按比例平仓的张数，按最小变动单位向下取整。凑不出合法张数或剩余太少时返回 None（改为全平）。"""
    if lot_sz <= 0:
        return None
    raw = pos_abs * Decimal(str(pct)) / Decimal(100)
    n = (raw / lot_sz).to_integral_value(rounding=ROUND_DOWN) * lot_sz
    if n <= 0 or n < min_sz or pos_abs - n < min_sz:
        return None
    return n


def split_size(total: Decimal, max_sz: Decimal | None, lot_sz: Decimal) -> list[Decimal]:
    """单笔市价单有张数上限，超出就拆成多笔。"""
    if not max_sz or max_sz <= 0 or total <= max_sz:
        return [total]
    max_sz = (max_sz / lot_sz).to_integral_value(rounding=ROUND_DOWN) * lot_sz
    out = []
    left = total
    while left > 0:
        c = min(left, max_sz)
        out.append(c)
        left -= c
    return out


def fmt_size(d: Decimal) -> str:
    return format(d.normalize(), "f")
