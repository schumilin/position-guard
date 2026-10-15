"""价格采样序列：按时间升序存 (时间戳毫秒, 价格)，支持按锚点时刻查基准价。"""
from __future__ import annotations

from bisect import bisect_left


class PriceSeries:
    __slots__ = ("ts", "px", "head")

    def __init__(self) -> None:
        self.ts: list[int] = []
        self.px: list[float] = []
        self.head = 0  # 之前的点已过期，等攒够了再一次性删除

    def __len__(self) -> int:
        return len(self.ts) - self.head

    def append(self, ts: int, px: float) -> None:
        if len(self) and ts <= self.ts[-1]:
            return
        self.ts.append(ts)
        self.px.append(px)

    def anchor(self, target_ts: int) -> tuple[int, float] | None:
        """锚点价：采样中时间 ≥ target_ts 且离它最近的那个点。"""
        i = bisect_left(self.ts, target_ts, lo=self.head)
        if i >= len(self.ts):
            return None
        return self.ts[i], self.px[i]

    def first_ts(self) -> int | None:
        return self.ts[self.head] if len(self) else None

    def last_ts(self) -> int | None:
        return self.ts[-1] if len(self) else None

    def prune(self, before_ts: int) -> None:
        self.head = bisect_left(self.ts, before_ts, lo=self.head)
        if self.head > 4096 and self.head * 2 > len(self.ts):
            del self.ts[: self.head]
            del self.px[: self.head]
            self.head = 0

    def backfill(self, points: list[tuple[int, float]]) -> int:
        """把历史点（比如分钟 K 线收盘价）补到最前面，只补比现有最早点更早的。"""
        first = self.first_ts()
        older = sorted(p for p in points if first is None or p[0] < first)
        dedup: list[tuple[int, float]] = []
        for p in older:
            if not dedup or p[0] > dedup[-1][0]:
                dedup.append(p)
        if not dedup:
            return 0
        self.ts[self.head:self.head] = [p[0] for p in dedup]
        self.px[self.head:self.head] = [p[1] for p in dedup]
        return len(dedup)
