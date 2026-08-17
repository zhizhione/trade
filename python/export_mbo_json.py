#!/usr/bin/env python3
"""将 Databento MBO DBN 文件流式导出为原始 JSON Lines。

每行是一条 MBO 记录，按 DBN 文件原始顺序写出。``price`` 保留 Databento 的
整数纳美元单位，避免 JSON 浮点数带来的精度损失；``source_ordinal`` 是该记录在
源 DBN 解码流中的零基位置，可用于后续精确回放。
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, TextIO

from import_dbn import RAW_COLUMNS, load_databento, open_mbo_store, raw_row


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", type=Path, help="输入 .mbo.dbn 或 .mbo.dbn.zst 文件")
    parser.add_argument(
        "--output",
        type=Path,
        help="输出 .jsonl 文件；未指定时写入 output/<输入文件名>.jsonl",
    )
    parser.add_argument("--overwrite", action="store_true", help="允许覆盖已有输出文件")
    return parser.parse_args()


def default_output_path(input_path: Path) -> Path:
    name = input_path.name
    for suffix in (".dbn.zst", ".dbn"):
        if name.endswith(suffix):
            name = name[: -len(suffix)]
            break
    return Path("output") / f"{name}.jsonl"


def json_row(source_ordinal: int, record: Any) -> dict[str, Any]:
    row = raw_row(record)
    return {"source_ordinal": source_ordinal, **dict(zip(RAW_COLUMNS, row, strict=True))}


def export_file(input_path: Path, output_path: Path, *, db: Any) -> tuple[int, int]:
    input_path = input_path.resolve()
    output_path = output_path.resolve()
    if not input_path.is_file():
        raise FileNotFoundError(f"输入文件不存在: {input_path}")
    if output_path.exists():
        raise FileExistsError(f"输出文件已存在: {output_path}；使用 --overwrite 覆盖")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = output_path.with_suffix(f"{output_path.suffix}.partial")
    if temporary_path.exists():
        raise FileExistsError(f"临时文件已存在: {temporary_path}；请确认后删除或改用其他输出路径")

    store = open_mbo_store(input_path, db)
    try:
        with temporary_path.open("x", encoding="utf-8", newline="\n") as destination:
            decoded_records, mbo_records = write_records(store, destination, db=db)
        temporary_path.replace(output_path)
    except BaseException:
        if temporary_path.exists():
            temporary_path.unlink()
        raise
    return decoded_records, mbo_records


def write_records(store: Any, destination: TextIO, *, db: Any) -> tuple[int, int]:
    """写出记录并返回解码总数和 MBO 行数。"""

    decoded_records = 0
    mbo_records = 0
    for source_ordinal, record in enumerate(store):
        decoded_records += 1
        if not isinstance(record, db.MBOMsg):
            continue
        json.dump(json_row(source_ordinal, record), destination, ensure_ascii=True, separators=(",", ":"))
        destination.write("\n")
        mbo_records += 1
    return decoded_records, mbo_records


def main() -> int:
    args = parse_args()
    input_path = args.input.resolve()
    output_path = args.output or default_output_path(input_path)
    if args.overwrite and output_path.exists():
        output_path.unlink()
    decoded_records, mbo_records = export_file(input_path, output_path, db=load_databento())
    print(f"export-mbo-json: input={input_path}")
    print(f"export-mbo-json: output={output_path.resolve()}")
    print(f"export-mbo-json: decoded={decoded_records} mbo={mbo_records}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
