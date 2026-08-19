#!/usr/bin/env python3
"""将 Databento MBO 重建的 L2 深度与官方 MBP-10/TBBO 文件逐记录对账。

对齐键是 ``(publisher_id, instrument_id, sequence, ts_event)``，而不是时间戳单独使用。
MBP-10 的 ``F_LAST`` 行对应 MBO publisher message 的结束；TBBO 的成交行则对应 MBO 的
``T`` 行，盘口状态为该成交发生前（``T/F`` 本身不变更挂单，后续 ``C`` 才表达被动单扣减）。

本脚本刻意使用与 ``MboBookEngine`` 相同的 A/M/C/R/T/F/N 语义作为独立审计实现。它不写入
ClickHouse，也不修改输入文件；任何差异都会输出可用原始键重新定位的证据。
"""

from __future__ import annotations

import argparse
import hashlib
import sys
from collections import OrderedDict
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal


F_LAST = 1 << 7
DEPTH = 10


@dataclass(frozen=True)
class Order:
    order_id: int
    side: str
    price: int
    size: int
    priority: int


@dataclass(frozen=True)
class Reference:
    source: Literal["mbp10", "tbbo"]
    sequence: int
    ts_event: int
    action: str
    side: str
    price: int
    size: int
    depth: int
    bids: tuple[tuple[int, int, int], ...]
    asks: tuple[tuple[int, int, int], ...]


class OrderBook:
    """只保留校验所需状态，状态迁移与 Java MboBookEngine 保持一致。"""

    def __init__(self) -> None:
        self.orders: dict[int, Order] = {}
        self.bids: dict[int, OrderedDict[int, Order]] = {}
        self.asks: dict[int, OrderedDict[int, Order]] = {}

    def apply(self, record: Any, source_ordinal: int) -> None:
        action = str(record.action)
        if action == "A":
            self._add(record, source_ordinal)
        elif action == "M":
            self._modify(record, source_ordinal)
        elif action == "C":
            self._cancel(record)
        elif action == "R":
            self.orders.clear()
            self.bids.clear()
            self.asks.clear()
        elif action not in {"T", "F", "N"}:
            raise ValidationError(f"unsupported MBO action={action}")

    def snapshot(self, depth: int = DEPTH) -> tuple[tuple[tuple[int, int, int], ...], tuple[tuple[int, int, int], ...]]:
        return self._aggregate(self.bids, reverse=True, depth=depth), self._aggregate(self.asks, reverse=False, depth=depth)

    def _add(self, record: Any, priority: int) -> None:
        order_id = int(record.order_id)
        side = str(record.side)
        size = int(record.size)
        if side not in {"B", "A"} or size <= 0:
            raise ValidationError(f"invalid Add: side={side}, size={size}")
        if order_id in self.orders:
            raise ValidationError(f"duplicate active order_id={order_id}")
        order = Order(order_id, side, int(record.price), size, priority)
        self.orders[order_id] = order
        self._level(order.side, order.price, create=True)[order_id] = order

    def _modify(self, record: Any, priority: int) -> None:
        order_id = int(record.order_id)
        old = self._require_order(order_id, "Modify")
        side = str(record.side)
        size = int(record.size)
        price = int(record.price)
        if side != old.side or size <= 0:
            raise ValidationError(f"invalid Modify for order_id={order_id}")
        loses_priority = old.price != price or old.size < size
        old_level = self._level(old.side, old.price, create=False)
        del old_level[order_id]
        self._remove_level_if_empty(old.side, old.price, old_level)
        updated = Order(order_id, old.side, price, size, priority if loses_priority else old.priority)
        self.orders[order_id] = updated
        new_level = self._level(updated.side, updated.price, create=True)
        if loses_priority:
            new_level[order_id] = updated
        else:
            self._insert_by_priority(new_level, updated)

    def _cancel(self, record: Any) -> None:
        order_id = int(record.order_id)
        old = self._require_order(order_id, "Cancel")
        side = str(record.side)
        price = int(record.price)
        size = int(record.size)
        if side != old.side or price != old.price or size <= 0 or size > old.size:
            raise ValidationError(f"invalid Cancel for order_id={order_id}")
        level = self._level(old.side, old.price, create=False)
        if size == old.size:
            del self.orders[order_id]
            del level[order_id]
            self._remove_level_if_empty(old.side, old.price, level)
            return
        reduced = Order(old.order_id, old.side, old.price, old.size - size, old.priority)
        self.orders[order_id] = reduced
        level[order_id] = reduced

    def _require_order(self, order_id: int, action: str) -> Order:
        try:
            return self.orders[order_id]
        except KeyError as error:
            raise ValidationError(f"{action} references unknown order_id={order_id}") from error

    def _level(self, side: str, price: int, *, create: bool) -> OrderedDict[int, Order]:
        levels = self.bids if side == "B" else self.asks
        if create:
            return levels.setdefault(price, OrderedDict())
        try:
            return levels[price]
        except KeyError as error:
            raise ValidationError(f"missing price level side={side}, price={price}") from error

    def _remove_level_if_empty(self, side: str, price: int, level: OrderedDict[int, Order]) -> None:
        if not level:
            (self.bids if side == "B" else self.asks).pop(price)

    def _insert_by_priority(self, level: OrderedDict[int, Order], updated: Order) -> None:
        rebuilt: OrderedDict[int, Order] = OrderedDict()
        inserted = False
        for order in level.values():
            if not inserted and updated.priority < order.priority:
                rebuilt[updated.order_id] = updated
                inserted = True
            rebuilt[order.order_id] = order
        if not inserted:
            rebuilt[updated.order_id] = updated
        level.clear()
        level.update(rebuilt)

    @staticmethod
    def _aggregate(
        levels: dict[int, OrderedDict[int, Order]], *, reverse: bool, depth: int
    ) -> tuple[tuple[int, int, int], ...]:
        result: list[tuple[int, int, int]] = []
        for price in sorted(levels, reverse=reverse):
            orders = levels[price]
            result.append((price, sum(order.size for order in orders.values()), len(orders)))
            if len(result) == depth:
                break
        return tuple(result)


class ValidationError(RuntimeError):
    pass


def level_rows(record: Any, side: Literal["bid", "ask"], depth: int) -> tuple[tuple[int, int, int], ...]:
    result: list[tuple[int, int, int]] = []
    for index in range(depth):
        price = int(getattr(record, f"{side}_px_{index:02}"))
        size = int(getattr(record, f"{side}_sz_{index:02}"))
        count = int(getattr(record, f"{side}_ct_{index:02}"))
        if size == 0 and count == 0:
            continue
        if price == 0 or size <= 0 or count <= 0:
            raise ValidationError(
                f"invalid official {side} level at sequence={record.sequence}, depth={index}: "
                f"price={price}, size={size}, count={count}"
            )
        result.append((price, size, count))
    return tuple(result)


def reference_rows(path: Path, source: Literal["mbp10", "tbbo"], *, last_only: bool) -> Iterator[Reference]:
    db = load_databento()
    depth = DEPTH if source == "mbp10" else 1
    for record in db.DBNStore.from_file(path):
        is_last = bool(int(record.flags) & F_LAST)
        if is_last != last_only:
            continue
        yield Reference(
            source=source,
            sequence=int(record.sequence),
            ts_event=int(record.ts_event),
            action=str(record.action),
            side=str(record.side),
            price=int(record.price),
            size=int(record.size),
            depth=int(record.depth),
            bids=level_rows(record, "bid", depth),
            asks=level_rows(record, "ask", depth),
        )


def reference_key(reference: Reference) -> tuple[int, int]:
    return reference.sequence, reference.ts_event


def mbo_key(record: Any) -> tuple[int, int]:
    return int(record.sequence), int(record.ts_event)


def compare(
    mbo_path: Path,
    mbp10_path: Path,
    tbbo_path: Path,
    *,
    max_examples: int,
) -> int:
    db = load_databento()
    mbp_rows = reference_rows(mbp10_path, "mbp10", last_only=True)
    tbbo_rows = reference_rows(tbbo_path, "tbbo", last_only=False)
    expected_mbp = next(mbp_rows, None)
    expected_tbbo = next(tbbo_rows, None)
    book = OrderBook()
    total_records = 0
    last_messages = 0
    trade_messages = 0
    matched_mbp = 0
    matched_tbbo = 0
    exact_mbp = 0
    exact_tbbo = 0
    mismatches: list[str] = []

    for source_ordinal, record in enumerate(db.DBNStore.from_file(mbo_path)):
        total_records += 1
        action = str(record.action)
        key = mbo_key(record)
        try:
            book.apply(record, source_ordinal)
        except ValidationError as error:
            raise ValidationError(
                f"MBO lifecycle failure at source_ordinal={source_ordinal}, key={key}, action={action}: {error}"
            ) from error

        if action == "T":
            trade_messages += 1
            expected_tbbo, matched_tbbo, exact_tbbo = compare_ready(
                "tbbo", expected_tbbo, tbbo_rows, key, record, book.snapshot(1), matched_tbbo, exact_tbbo, mismatches,
                max_examples,
            )

        if int(record.flags) & F_LAST:
            last_messages += 1
            expected_mbp, matched_mbp, exact_mbp = compare_ready(
                "mbp10", expected_mbp, mbp_rows, key, record, book.snapshot(DEPTH), matched_mbp, exact_mbp, mismatches,
                max_examples,
            )

    if expected_mbp is not None:
        raise ValidationError(f"MBO ended before MBP-10 reference key={reference_key(expected_mbp)}")
    if expected_tbbo is not None:
        raise ValidationError(f"MBO ended before TBBO reference key={reference_key(expected_tbbo)}")

    print(f"MBO records: {total_records}")
    print(f"MBO F_LAST messages: {last_messages}")
    print(f"MBO trade messages: {trade_messages}")
    print(f"MBP-10 matched/exact: {matched_mbp}/{exact_mbp}")
    print(f"TBBO matched/exact: {matched_tbbo}/{exact_tbbo}")
    print(f"Mismatch count: {matched_mbp + matched_tbbo - exact_mbp - exact_tbbo}")
    for mismatch in mismatches:
        print(mismatch)
    return 0 if exact_mbp == matched_mbp and exact_tbbo == matched_tbbo else 1


def compare_ready(
    source: Literal["mbp10", "tbbo"],
    expected: Reference | None,
    rows: Iterator[Reference],
    key: tuple[int, int],
    record: Any,
    actual: tuple[tuple[tuple[int, int, int], ...], tuple[tuple[int, int, int], ...]],
    matched: int,
    exact: int,
    mismatches: list[str],
    max_examples: int,
) -> tuple[Reference | None, int, int]:
    while expected is not None and reference_key(expected) < key:
        raise ValidationError(f"missing {source} key={reference_key(expected)} before MBO key={key}")
    if expected is None or reference_key(expected) != key:
        return expected, matched, exact
    # MBP-10 intentionally omits MBO updates outside its visible ten levels. Multiple MBO
    # records can therefore share one sequence/timestamp while only a subset has a reference row.
    # Match the official event identity before consuming the reference snapshot. MBP side is
    # schema-specific and does not consistently mean the MBO resting-order side (for example an
    # official C/B row can correspond to an MBO cancellation of an Ask); do not use it as a key.
    initial_snapshot = expected.source == "mbp10" and expected.side == "N"
    if not initial_snapshot and (
        expected.action != str(record.action)
        or expected.price != int(record.price)
        or expected.size != int(record.size)
    ):
        return expected, matched, exact
    matched += 1
    if actual == (expected.bids, expected.asks):
        exact += 1
    elif len(mismatches) < max_examples:
        mismatches.append(
            f"{source} mismatch key={key}: expected={format_snapshot(expected.bids, expected.asks)} "
            f"actual={format_snapshot(*actual)}"
        )
    return next(rows, None), matched, exact


def format_snapshot(
    bids: tuple[tuple[int, int, int], ...], asks: tuple[tuple[int, int, int], ...]
) -> str:
    return f"bids={list(bids)} asks={list(asks)}"


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_databento() -> Any:
    try:
        import databento as db
    except ImportError as error:
        raise RuntimeError("未安装 databento；请安装 python/requirements.txt") from error
    return db


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mbo", type=Path, required=True, help="同日官方 MBO DBN 文件")
    parser.add_argument("--mbp10", type=Path, required=True, help="同日官方 MBP-10 DBN 文件")
    parser.add_argument("--tbbo", type=Path, required=True, help="同日官方 TBBO DBN 文件")
    parser.add_argument("--max-examples", type=int, default=5, help="最多输出的差异样本数")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    paths = (args.mbo, args.mbp10, args.tbbo)
    for path in paths:
        if not path.is_file():
            raise FileNotFoundError(path)
    if args.max_examples < 1:
        raise ValueError("max-examples must be positive")
    print(f"MBO SHA256: {sha256(args.mbo)}")
    print(f"MBP-10 SHA256: {sha256(args.mbp10)}")
    print(f"TBBO SHA256: {sha256(args.tbbo)}")
    return compare(args.mbo, args.mbp10, args.tbbo, max_examples=args.max_examples)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, RuntimeError, ValidationError, ValueError) as error:
        print(f"validation failed: {error}", file=sys.stderr)
        raise SystemExit(2)
