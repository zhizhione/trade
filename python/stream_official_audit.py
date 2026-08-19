#!/usr/bin/env python3
"""Decode official DBN files and feed the real Java MboBookEngine audit process."""

from __future__ import annotations

import argparse
import subprocess
import sys
from collections import deque
from pathlib import Path
from typing import Any

F_LAST = 1 << 7
DEPTH = 10


def levels(record: Any, side: str, depth: int) -> str:
    result: list[str] = []
    for index in range(depth):
        price = int(getattr(record, f"{side}_px_{index:02}"))
        size = int(getattr(record, f"{side}_sz_{index:02}"))
        count = int(getattr(record, f"{side}_ct_{index:02}"))
        if size == 0 and count == 0:
            continue
        result.append(f"{price}:{size}:{count}")
    return ",".join(result)


def key(record: Any) -> tuple[int, int]:
    return int(record.sequence), int(record.ts_event)


class ReferenceStream:
    def __init__(self, path: Path, source: str, last_only: bool) -> None:
        import databento as db

        self.source = source
        self.depth = DEPTH if source == "mbp10" else 1
        self._records = iter(db.DBNStore.from_file(path))
        self._last_only = last_only
        self._lookahead: Any | None = None
        self._pending: deque[Any] = deque()

    def _next_filtered(self) -> Any | None:
        for record in self._records:
            if bool(int(record.flags) & F_LAST) == self._last_only:
                return record
        return None

    def _ensure_lookahead(self) -> None:
        if self._lookahead is None:
            self._lookahead = self._next_filtered()

    def consume_for(self, mbo: Any) -> Any | None:
        target = key(mbo)
        if self._pending and key(self._pending[0]) < target:
            raise RuntimeError(
                f"unmatched {self.source} reference key={key(self._pending[0])} before MBO key={target}"
            )
        self._ensure_lookahead()
        if self._lookahead is not None and key(self._lookahead) < target:
            raise RuntimeError(
                f"unmatched {self.source} reference key={key(self._lookahead)} before MBO key={target}"
            )
        while self._lookahead is not None and key(self._lookahead) == target:
            self._pending.append(self._lookahead)
            self._lookahead = self._next_filtered()
        for reference in list(self._pending):
            if matches(reference, mbo):
                self._pending.remove(reference)
                return reference
        return None

    def finish(self) -> None:
        self._ensure_lookahead()
        if self._pending:
            raise RuntimeError(f"unmatched {self.source} reference key={key(self._pending[0])}")
        if self._lookahead is not None:
            raise RuntimeError(f"unconsumed {self.source} reference key={key(self._lookahead)}")


def event_line(source_ordinal: int, record: Any) -> str:
    return "|".join([
        "M", str(source_ordinal), str(int(record.ts_recv)), str(int(record.ts_event)),
        str(int(record.rtype)), str(int(record.publisher_id)), str(int(record.instrument_id)),
        str(record.action), str(record.side), str(int(record.price)), str(int(record.size)),
        str(int(record.channel_id)), str(int(record.order_id)), str(int(record.flags)),
        str(int(record.ts_in_delta)), str(int(record.sequence)),
    ])


def reference_line(source: str, record: Any, depth: int) -> str:
    return "|".join([
        "R", source, str(int(record.publisher_id)), str(int(record.instrument_id)),
        str(int(record.sequence)), str(int(record.ts_event)), str(depth),
        levels(record, "bid", depth), levels(record, "ask", depth),
    ])


def matches(reference: Any, mbo: Any) -> bool:
    # MBP/TBBO side is not the MBO resting-order side. Initial MBP snapshot is A/N.
    return (
        str(reference.action) == "A" and str(reference.side) == "N"
        or (
            str(reference.action) == str(mbo.action)
            and int(reference.price) == int(mbo.price)
            and int(reference.size) == int(mbo.size)
        )
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mbo", type=Path, required=True)
    parser.add_argument("--mbp10", type=Path, required=True)
    parser.add_argument("--tbbo", type=Path, required=True)
    parser.add_argument("--java-command", nargs="+", default=["./gradlew", "officialOrderBookAudit"])
    args = parser.parse_args()
    for path in (args.mbo, args.mbp10, args.tbbo):
        if not path.is_file():
            raise FileNotFoundError(path)

    import databento as db

    mbp = ReferenceStream(args.mbp10, "mbp10", True)
    tbbo = ReferenceStream(args.tbbo, "tbbo", False)
    process = subprocess.Popen(args.java_command, cwd=Path(__file__).parent.parent / "backend", stdin=subprocess.PIPE, text=True)
    assert process.stdin is not None
    try:
        for source_ordinal, record in enumerate(db.DBNStore.from_file(args.mbo)):
            process.stdin.write(event_line(source_ordinal, record) + "\n")
            if str(record.action) == "T":
                emit_reference(process, tbbo, record)
            if int(record.flags) & F_LAST:
                emit_reference(process, mbp, record)
        process.stdin.close()
        mbp.finish()
        tbbo.finish()
        return process.wait()
    finally:
        if process.poll() is None:
            process.kill()


def emit_reference(process: subprocess.Popen[str], references: ReferenceStream, record: Any) -> None:
    reference = references.consume_for(record)
    if reference is not None:
        process.stdin.write(reference_line(references.source, reference, references.depth) + "\n")


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, RuntimeError, ValueError) as error:
        print(f"audit stream failed: {error}", file=sys.stderr)
        raise SystemExit(2)
