#!/usr/bin/env python3
"""Export an inclusive ts_event interval from an MBO DBN file as one JSON document."""

from __future__ import annotations

import argparse
import calendar
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from import_dbn import RAW_COLUMNS, load_databento, open_mbo_store, raw_row

UTC_TIMESTAMP = re.compile(
    r"^(?P<second>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.(?P<fraction>\d{1,9}))?Z$"
)


def parse_utc_ns(value: str) -> int:
    """Parse an RFC 3339 UTC timestamp without losing sub-microsecond precision."""

    match = UTC_TIMESTAMP.fullmatch(value)
    if match is None:
        raise argparse.ArgumentTypeError("timestamp must be RFC 3339 UTC, for example 2026-08-06T08:19:39.8560400Z")
    second = datetime.strptime(match.group("second"), "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
    fraction = (match.group("fraction") or "").ljust(9, "0")
    return calendar.timegm(second.utctimetuple()) * 1_000_000_000 + int(fraction)


def format_utc_ns(timestamp_ns: int) -> str:
    seconds, nanos = divmod(timestamp_ns, 1_000_000_000)
    timestamp = datetime.fromtimestamp(seconds, tz=timezone.utc)
    return timestamp.strftime("%Y-%m-%dT%H:%M:%S") + f".{nanos:09d}Z"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", type=Path, help="input .mbo.dbn or .mbo.dbn.zst file")
    parser.add_argument("--start", type=parse_utc_ns, required=True, help="inclusive ts_event UTC timestamp")
    parser.add_argument("--end", type=parse_utc_ns, required=True, help="inclusive ts_event UTC timestamp")
    parser.add_argument("--output", type=Path, required=True, help="output .json file")
    return parser.parse_args()


def json_record(source_ordinal: int, record: Any) -> dict[str, Any]:
    values = dict(zip(RAW_COLUMNS, raw_row(record), strict=True))
    values["ts_event_iso"] = format_utc_ns(values["ts_event"])
    return {"source_ordinal": source_ordinal, **values}


def export_window(input_path: Path, output_path: Path, start_ns: int, end_ns: int) -> int:
    if start_ns > end_ns:
        raise ValueError("--start must be no later than --end")
    input_path = input_path.resolve()
    output_path = output_path.resolve()
    if not input_path.is_file():
        raise FileNotFoundError(input_path)
    if output_path.exists():
        raise FileExistsError(f"output already exists: {output_path}")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = output_path.with_suffix(output_path.suffix + ".partial")
    if temporary_path.exists():
        raise FileExistsError(f"temporary output already exists: {temporary_path}")

    db = load_databento()
    count = 0
    try:
        with temporary_path.open("x", encoding="utf-8", newline="\n") as destination:
            metadata = {
                "source_file": str(input_path),
                "time_field": "ts_event",
                "start_ts_event": start_ns,
                "start_ts_event_iso": format_utc_ns(start_ns),
                "end_ts_event": end_ns,
                "end_ts_event_iso": format_utc_ns(end_ns),
            }
            destination.write("{\"metadata\":")
            json.dump(metadata, destination, ensure_ascii=True, separators=(",", ":"))
            destination.write(",\"records\":[")
            for source_ordinal, record in enumerate(open_mbo_store(input_path, db)):
                if not isinstance(record, db.MBOMsg):
                    continue
                timestamp_ns = int(record.ts_event)
                if not start_ns <= timestamp_ns <= end_ns:
                    continue
                if count:
                    destination.write(",")
                json.dump(json_record(source_ordinal, record), destination, ensure_ascii=True, separators=(",", ":"))
                count += 1
            destination.write("]}\n")
        temporary_path.replace(output_path)
    except BaseException:
        if temporary_path.exists():
            temporary_path.unlink()
        raise
    return count


def main() -> int:
    args = parse_args()
    count = export_window(args.input, args.output, args.start, args.end)
    print(f"exported={count}")
    print(f"output={args.output.resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
