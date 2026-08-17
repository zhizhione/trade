#!/usr/bin/env python3
"""为已导入的 Databento DBN 文件补写文件目录，不修改原始事件表。

该脚本只重新解码源文件计算目录元数据，并用 ``dbn_import_jobs`` 的完成状态和
已提交行数做闸门校验。它不能从 raw 表反推出可靠的交易日或文件内首尾位置。
"""

from __future__ import annotations

import argparse
from pathlib import Path

from dotenv import load_dotenv

from import_dbn import (
    catalog_entries_from_manifest,
    calculate_file_sha256,
    create_clickhouse_client,
    expand_input_paths,
    load_databento,
    load_job,
    save_catalog_entry,
    scan_source_file,
    utc_now,
    validate_file_order,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("paths", nargs="+", help="已导入的 DBN 文件、目录或 glob")
    parser.add_argument(
        "--file-order",
        type=int,
        default=0,
        help="写入目录的显式文件顺序（默认 0）",
    )
    return parser.parse_args()


def backfill_file_catalog(client: object, path: Path, *, db: object, file_order: int = 0) -> int:
    """扫描源文件，确认 raw 导入完成后更新其目录行并返回 MBO 行数。

    目录写入放在 raw 成功之后；因此目录中的 ``status=completed`` 只代表对应
    文件已经通过导入任务的行数校验，而不是一个独立的事件存储。
    """

    validate_file_order(file_order)
    path = path.resolve()
    file_sha256 = calculate_file_sha256(path)
    job = load_job(client, file_sha256)
    if job is None or job.status != "completed":
        raise RuntimeError(f"文件尚未成功导入，不写目录: {path.name}")

    manifest = scan_source_file(path, db=db)
    if job.committed_rows != manifest.mbo_records:
        raise RuntimeError(
            f"文件目录回填校验失败: {path.name} "
            f"jobs.committed_rows={job.committed_rows} decoded_mbo_rows={manifest.mbo_records}"
        )
    for entry in catalog_entries_from_manifest(
            path,
            file_sha256,
            manifest,
            updated_at=utc_now(),
            file_order=file_order,
    ):
        save_catalog_entry(client, entry)
    return manifest.mbo_records


def main() -> int:
    load_dotenv()
    args = parse_args()
    try:
        validate_file_order(args.file_order)
    except ValueError as exception:
        raise SystemExit(f"错误: {exception}") from exception
    paths = expand_input_paths(args.paths)
    db = load_databento()
    client = create_clickhouse_client()
    total = 0
    for path in paths:
        rows = backfill_file_catalog(client, path, db=db, file_order=args.file_order)
        total += rows
        print(f"mbo-file-catalog-backfill: file={path.name} mbo_rows={rows}")
    print(f"mbo-file-catalog-backfill: files={len(paths)} mbo_rows={total}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
