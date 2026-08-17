#!/usr/bin/env python3
"""将 Databento 历史 MBO DBN 文件直接、可恢复地导入 ClickHouse。

历史文件不经过 Kafka：脚本把文件身份、文件内顺序和 DBN 的 14 个官方原始字段
写入 ``databento_mbo_raw``。``(file_sha256, source_ordinal)`` 是事件的稳定身份；
展示名称和特征属于下游表，避免污染可重放的原始事实数据。

每个文件以其字节 SHA-256 作为导入身份，并依次经历领取、暂存、校验、提交
和完成。这样在进程中断或重跑时，可以跳过已完成文件，而不会将未校验的半份
数据写入正式表。
"""

from __future__ import annotations

import argparse
import glob
import hashlib
import os
import re
import socket
import struct
import sys
import time
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

from dotenv import load_dotenv


DBN_SUFFIXES = (".dbn", ".dbn.zst")
DEFAULT_BATCH_SIZE = 10_000
DEFAULT_LEASE_SECONDS = 6 * 60 * 60
RAW_TABLE = "databento_mbo_raw"
JOB_TABLE = "dbn_import_jobs"
CATALOG_TABLE = "databento_mbo_file_catalog"
STAGING_TABLE_PREFIX = "databento_mbo_stage_"
ACTIVE_JOB_STATUSES = frozenset({"claimed", "staging", "committing"})

# 与 Databento 官方 MBO 二进制记录一一对应。这个元组是 Python、暂存表、正式表
# 和内容摘要之间的数据契约；调整顺序或字段时，必须同步检查 ClickHouse 建表、
# Java ResultSet 读取和测试夹具，不能只修改其中一个调用方。
RAW_COLUMNS = (
    "ts_recv",
    "ts_event",
    "rtype",
    "publisher_id",
    "instrument_id",
    "action",
    "side",
    "price",
    "size",
    "channel_id",
    "order_id",
    "flags",
    "ts_in_delta",
    "sequence",
)
# 身份列不属于 Databento 的 14 个原始字段：SHA 标识源文件，ordinal 保留 DBN
# 解码流位置。两者合起来才能在重复文件、乱序时间戳和 sequence 重置时稳定定位一行。
IDENTITY_COLUMNS = ("file_sha256", "source_ordinal")
STORAGE_COLUMNS = (*IDENTITY_COLUMNS, *RAW_COLUMNS)

JOB_COLUMNS = (
    "file_sha256",
    "source_path",
    "display_name",
    "file_size",
    "status",
    "expected_rows",
    "staged_rows",
    "committed_rows",
    "error_message",
    "attempt",
    "claimed_by",
    "claim_token",
    "lease_expires_at",
    "started_at",
    "updated_at",
    "completed_at",
    "version",
)

CATALOG_COLUMNS = (
    "file_sha256",
    "publisher_id",
    "instrument_id",
    "source_path",
    "display_name",
    "trading_date",
    "file_order",
    "file_size",
    "decoded_rows",
    "mbo_rows",
    "skipped_rows",
    "first_ts_event",
    "last_ts_event",
    "min_ts_event",
    "max_ts_event",
    "first_source_ordinal",
    "last_source_ordinal",
    "status",
    "updated_at",
    "version",
)
DBN_TRADING_DATE_PATTERN = re.compile(r"(?:^|-)(\d{8})(?=\.mbo\.dbn(?:\.zst)?$)")

# 按官方字段类型编码单行，摘要与 ClickHouse 返回顺序无关。
RAW_ROW_STRUCT = struct.Struct(">QQBHIccqIBQBiI")
CHECKSUM_MASK = (1 << 64) - 1
CHECKSUM_PART_COUNT = 4


@dataclass
class ImportStats:
    """本次命令的累积统计，不作为数据库中的任务状态来源。"""
    files: int = 0
    completed_files: int = 0
    duplicate_files: int = 0
    failed_files: int = 0
    decoded_records: int = 0
    mbo_records: int = 0
    skipped_records: int = 0
    staged_records: int = 0
    inserted_records: int = 0
    raw_batches: int = 0


@dataclass(frozen=True)
class ContentChecksum:
    """用于比对源文件和暂存表内容的无序聚合摘要。

每行 SHA-256 被拆为四段 UInt64；每段同时计算模 2^64 的和与异或。两种聚合
均与行顺序无关，因而可在 ClickHouse 内计算，不必把暂存表全量读回 Python。
它用于发现漏行、重行和字段变化，行数则作为额外的独立校验。
"""
    sums: tuple[int, int, int, int]
    xors: tuple[int, int, int, int]

    @classmethod
    def empty(cls) -> "ContentChecksum":
        return cls((0,) * CHECKSUM_PART_COUNT, (0,) * CHECKSUM_PART_COUNT)

    @property
    def display(self) -> str:
        sums = ":".join(f"{value:016x}" for value in self.sums)
        xors = ":".join(f"{value:016x}" for value in self.xors)
        return f"sum={sums};xor={xors}"

    def add(self, parts: tuple[int, int, int, int]) -> "ContentChecksum":
        return ContentChecksum(
            tuple((left + right) & CHECKSUM_MASK for left, right in zip(self.sums, parts, strict=True)),
            tuple(left ^ right for left, right in zip(self.xors, parts, strict=True)),
        )


@dataclass(frozen=True)
class ContentManifest:
    """一次解码或暂存扫描的记录数量和内容摘要。"""
    decoded_records: int
    mbo_records: int
    skipped_records: int
    checksum: ContentChecksum
    first_ts_event: int | None = None
    last_ts_event: int | None = None
    min_ts_event: int | None = None
    max_ts_event: int | None = None
    first_source_ordinal: int | None = None
    last_source_ordinal: int | None = None
    identity_rows: tuple[tuple[int, int, int], ...] = ()


@dataclass(frozen=True)
class FileCatalogEntry:
    """一个可重放文件/Databento 身份组合的目录行；不承载事件明细。"""

    file_sha256: str
    publisher_id: int
    instrument_id: int
    source_path: str
    display_name: str
    trading_date: date | None
    file_order: int
    file_size: int
    decoded_rows: int
    mbo_rows: int
    skipped_rows: int
    first_ts_event: int | None
    last_ts_event: int | None
    min_ts_event: int | None
    max_ts_event: int | None
    first_source_ordinal: int | None
    last_source_ordinal: int | None
    status: str
    updated_at: datetime
    version: int


@dataclass(frozen=True)
class ImportJob:
    """``dbn_import_jobs`` 中一个文件的当前状态版本。"""
    file_sha256: str
    source_path: str
    display_name: str
    file_size: int
    status: str
    expected_rows: int | None
    staged_rows: int
    committed_rows: int
    error_message: str
    attempt: int
    claimed_by: str
    claim_token: UUID | None
    lease_expires_at: datetime | None
    started_at: datetime | None
    updated_at: datetime
    completed_at: datetime | None
    version: int


class ContentIntegrityError(RuntimeError):
    """Raised when staged content does not match the decoded DBN source."""


class ImportInProgressError(RuntimeError):
    """Raised when another worker still owns an unexpired file lease."""


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "paths",
        nargs="+",
        help="DBN 文件、包含 DBN 文件的目录，或 glob 模式",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help=f"每批 ClickHouse 写入的最多记录数（默认 {DEFAULT_BATCH_SIZE}）",
    )
    parser.add_argument(
        "--max-records",
        type=int,
        default=0,
        help="dry-run 时每个文件最多解码的记录数；实际导入禁止使用",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="验证 DBN MBO 记录，不写入 ClickHouse",
    )
    parser.add_argument(
        "--file-order",
        type=int,
        default=0,
        help="文件目录中的显式顺序（默认 0；同一交易日多文件回放时使用）",
    )
    return parser.parse_args(argv)


def expand_input_paths(inputs: Sequence[str]) -> list[Path]:
    """展开文件、目录和 glob，并按解析后的绝对路径去重。

去重发生在命令入口，避免同一文件同时通过目录和 glob 参数出现时被重复处理；
真正的跨进程幂等身份仍然是后续计算的文件 SHA-256。
"""

    paths: list[Path] = []
    seen: set[Path] = set()
    for raw_input in inputs:
        matches = [Path(raw_input)]
        if any(character in raw_input for character in "*?[]"):
            matches = [Path(match) for match in glob.glob(raw_input, recursive=True)]
        if not matches:
            raise FileNotFoundError(f"输入路径不存在或没有匹配文件: {raw_input}")

        for match in matches:
            if match.is_dir():
                candidates: Iterable[Path] = (
                    path for path in match.rglob("*") if path.is_file() and is_dbn_file(path)
                )
            elif match.is_file():
                if not is_dbn_file(match):
                    raise ValueError(f"不是支持的 DBN 文件: {match}")
                candidates = (match,)
            else:
                raise FileNotFoundError(f"输入路径不存在: {match}")

            for candidate in sorted(candidates):
                resolved = candidate.resolve()
                if resolved not in seen:
                    seen.add(resolved)
                    paths.append(resolved)

    if not paths:
        raise FileNotFoundError("没有找到 DBN 文件")
    return paths


def is_dbn_file(path: Path) -> bool:
    return path.name.lower().endswith(DBN_SUFFIXES)


def create_clickhouse_client() -> Any:
    """按环境变量创建客户端；数据库名只在这里集中决定。"""
    try:
        import clickhouse_connect
    except ImportError as exception:
        raise RuntimeError(
            "未安装 clickhouse-connect，请先运行 `python -m pip install -r requirements.txt`"
        ) from exception

    return clickhouse_connect.get_client(
        host=os.getenv("CLICKHOUSE_HOST", "localhost"),
        port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
        username=os.getenv("CLICKHOUSE_USERNAME", "default"),
        password=os.getenv("CLICKHOUSE_PASSWORD", ""),
        database=os.getenv("CLICKHOUSE_DATABASE", "market_data"),
    )


def load_databento() -> Any:
    try:
        import databento as db
    except ImportError as exception:
        raise RuntimeError(
            "未安装 databento，请先运行 `python -m pip install -r requirements.txt`"
        ) from exception
    return db


def import_files(
    client: Any | None,
    paths: Sequence[Path],
    *,
    db: Any,
    batch_size: int,
    max_records: int = 0,
    dry_run: bool = False,
    file_order: int = 0,
) -> ImportStats:
    """逐文件导入，使一个 SHA-256 对应一条可恢复的持久化任务。"""

    if max_records and not dry_run:
        raise ValueError("--max-records 只能与 --dry-run 一起使用")

    stats = ImportStats(files=len(paths))
    for path in paths:
        import_file(
            client,
            path,
            db=db,
            batch_size=batch_size,
            max_records=max_records,
            dry_run=dry_run,
            file_order=file_order,
            stats=stats,
        )
    return stats


def import_file(
    client: Any | None,
    path: Path,
    *,
    db: Any,
    batch_size: int,
    max_records: int = 0,
    dry_run: bool = False,
    stats: ImportStats | None = None,
    worker_id: str | None = None,
    lease_seconds: int = DEFAULT_LEASE_SECONDS,
    file_order: int = 0,
) -> ImportStats:
    """完整验证一个 DBN 文件的暂存内容后，再一次性提交到正式表。

正常导入只解码源文件一次：同一遍扫描同时写暂存表并计算源内容摘要。完成后
由 ClickHouse 对暂存表计算等价摘要；两者一致才允许提交。任何异常（包括
Ctrl-C）都会将任务标记失败并尽力删除暂存表，正式表不会接收未验证数据。
"""

    if batch_size < 1:
        raise ValueError("batch_size 必须是正整数")
    if max_records < 0:
        raise ValueError("max_records 不能小于 0")
    if max_records and not dry_run:
        raise ValueError("--max-records 只能与 --dry-run 一起使用")
    if lease_seconds < 1:
        raise ValueError("lease_seconds 必须是正整数")
    validate_file_order(file_order)

    stats = stats or ImportStats(files=1)
    path = path.resolve()

    if dry_run:
        manifest = scan_source_file(path, db=db, max_records=max_records)
        add_source_stats(stats, manifest)
        return stats

    if client is None:
        raise RuntimeError("写入 ClickHouse 时缺少客户端")

    file_sha256 = calculate_file_sha256(path)
    job = claim_import_job(
        client,
        path,
        file_sha256,
        worker_id=worker_id or default_worker_id(),
        lease_seconds=lease_seconds,
    )
    if job is None:
        stats.duplicate_files += 1
        return stats

    staging_table = staging_table_name(file_sha256)
    staging_created = False
    staged_rows = 0
    try:
        # 领取令牌在每个会改变数据可见性的阶段前复查，租约失效后旧 worker
        # 不能继续覆盖新 worker 的任务状态或提交其暂存结果。
        job = transition_job(
            job,
            "staging",
            lease_seconds=lease_seconds,
            expected_rows=None,
            staged_rows=0,
            committed_rows=0,
            error_message="",
        )
        save_job(client, job)
        assert_claim(client, file_sha256, job.claim_token)

        reset_staging_table(client, staging_table)
        staging_created = True
        staged_manifest, batch_count = load_staging_file(
            client,
            staging_table,
            path,
            db=db,
            file_sha256=file_sha256,
            batch_size=batch_size,
        )
        source_manifest = staged_manifest
        add_source_stats(stats, source_manifest)
        staged_rows = staged_manifest.mbo_records
        stats.staged_records += staged_rows
        stats.raw_batches += batch_count
        # 先校验行数和内容摘要，再检查源文件哈希是否在导入期间发生变化。
        persisted_manifest = read_staging_manifest(client, staging_table)
        verify_staged_content(source_manifest, persisted_manifest)
        if calculate_file_sha256(path) != file_sha256:
            raise ContentIntegrityError(f"导入期间文件内容发生变化: {path}")

        job = transition_job(
            job,
            "staging",
            lease_seconds=lease_seconds,
            expected_rows=source_manifest.mbo_records,
            staged_rows=staged_rows,
        )
        save_job(client, job)
        assert_claim(client, file_sha256, job.claim_token)

        job = transition_job(job, "committing", lease_seconds=lease_seconds)
        save_job(client, job)
        assert_claim(client, file_sha256, job.claim_token)
        commit_staging_table(client, staging_table, file_sha256=file_sha256)

        completed_at = utc_now()
        for entry in catalog_entries_from_manifest(
            path,
            file_sha256,
            source_manifest,
            updated_at=completed_at,
            file_order=file_order,
        ):
            save_catalog_entry(client, entry)
        job = transition_job(
            job,
            "completed",
            lease_seconds=lease_seconds,
            committed_rows=staged_rows,
            completed_at=completed_at,
            claim_token=None,
            lease_expires_at=None,
        )
        save_job(client, job)
        stats.completed_files += 1
        stats.inserted_records += staged_rows
    except (Exception, KeyboardInterrupt) as exception:
        stats.failed_files += 1
        error_message = "导入被用户中断" if isinstance(exception, KeyboardInterrupt) else str(exception)
        mark_job_failed(
            client,
            job,
            file_sha256=file_sha256,
            staging_table=staging_table,
            staging_created=staging_created,
            staged_rows=staged_rows,
            lease_seconds=lease_seconds,
            error_message=error_message,
        )
        raise
    else:
        try:
            drop_staging_table(client, staging_table)
        except Exception as exception:
            print(f"警告: 已完成任务但清理 staging 表失败: {exception}", file=sys.stderr)
    return stats


def raw_row(record: Any) -> list[Any]:
    """按官方字段顺序返回一条原始 MBO 记录，不做映射或派生。"""

    return [
        int(record.ts_recv),
        int(record.ts_event),
        int(record.rtype),
        int(record.publisher_id),
        int(record.instrument_id),
        fixed_char(record.action, "action"),
        fixed_char(record.side, "side"),
        int(record.price),
        int(record.size),
        int(record.channel_id),
        int(record.order_id),
        int(record.flags),
        int(record.ts_in_delta),
        int(record.sequence),
    ]


def storage_row(file_sha256: str, source_ordinal: int, record: Any) -> list[Any]:
    """返回带稳定文件身份和 DBN 解码流零基序号的存储行。"""

    validate_record_identity(file_sha256, source_ordinal)
    return [file_sha256, source_ordinal, *raw_row(record)]


def mark_job_failed(
    client: Any,
    job: ImportJob,
    *,
    file_sha256: str,
    staging_table: str,
    staging_created: bool,
    staged_rows: int,
    lease_seconds: int,
    error_message: str,
) -> None:
    """仅当本 worker 仍持有租约时记录失败并清理本文件的暂存表。"""
    if not claim_is_owned(client, file_sha256, job.claim_token):
        return
    failed_job = transition_job(
        job,
        "failed",
        lease_seconds=lease_seconds,
        staged_rows=staged_rows,
        committed_rows=0,
        error_message=error_message[:4096],
        claim_token=None,
        lease_expires_at=None,
    )
    save_job(client, failed_job)
    if staging_created:
        try:
            drop_staging_table(client, staging_table)
        except Exception as cleanup_exception:
            print(f"警告: 失败后清理 staging 表失败: {cleanup_exception}", file=sys.stderr)


def calculate_file_sha256(path: Path, *, chunk_size: int = 1024 * 1024) -> str:
    """返回文件实际字节的 SHA-256，不解压也不依赖路径或文件名。"""

    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()


def scan_source_file(path: Path, *, db: Any, max_records: int = 0) -> ContentManifest:
    """供 dry-run 使用的只读扫描；``max_records`` 仅在此模式生效。

    ``first_ts_event``/``last_ts_event`` 表示源文件中第一条和最后一条 MBO 的
    原始位置，不是 min/max 时间。Databento 日文件可能以 snapshot 开始，因此
    事件时间早于文件名交易日并不表示导入错误。
    """
    store = open_mbo_store(path, db)
    decoded_records = 0
    mbo_records = 0
    skipped_records = 0
    checksum = ContentChecksum.empty()
    first_ts_event: int | None = None
    last_ts_event: int | None = None
    min_ts_event: int | None = None
    max_ts_event: int | None = None
    first_source_ordinal: int | None = None
    last_source_ordinal: int | None = None
    identity_rows: dict[tuple[int, int], int] = {}
    for record in store:
        if max_records and decoded_records >= max_records:
            break
        decoded_records += 1
        if not isinstance(record, db.MBOMsg):
            skipped_records += 1
            continue
        row = raw_row(record)
        mbo_records += 1
        identity = (int(row[3]), int(row[4]))
        identity_rows[identity] = identity_rows.get(identity, 0) + 1
        checksum = add_row_checksum(checksum, row)
        first_ts_event = row[1] if first_ts_event is None else first_ts_event
        last_ts_event = row[1]
        min_ts_event = row[1] if min_ts_event is None else min(min_ts_event, row[1])
        max_ts_event = row[1] if max_ts_event is None else max(max_ts_event, row[1])
        first_source_ordinal = decoded_records - 1 if first_source_ordinal is None else first_source_ordinal
        last_source_ordinal = decoded_records - 1
    return ContentManifest(
        decoded_records, mbo_records, skipped_records, checksum,
        first_ts_event, last_ts_event, min_ts_event, max_ts_event,
        first_source_ordinal, last_source_ordinal, catalog_identity_rows(identity_rows),
    )


def load_staging_file(
    client: Any,
    staging_table: str,
    path: Path,
    *,
    db: Any,
    file_sha256: str,
    batch_size: int,
) -> tuple[ContentManifest, int]:
    """单遍解码源文件，分批写入暂存表并同步计算源内容摘要。

    ``source_ordinal`` 使用 enumerate 的 DBN 解码流位置，而不是 MBO 行号；
    非 MBO 记录仍会占用位置，这样回放可以重现原始消息边界和顺序。
    """
    store = open_mbo_store(path, db)
    decoded_records = 0
    mbo_records = 0
    skipped_records = 0
    checksum = ContentChecksum.empty()
    first_ts_event: int | None = None
    last_ts_event: int | None = None
    min_ts_event: int | None = None
    max_ts_event: int | None = None
    first_source_ordinal: int | None = None
    last_source_ordinal: int | None = None
    identity_rows: dict[tuple[int, int], int] = {}
    batch_count = 0
    batch: list[list[Any]] = []
    for source_ordinal, record in enumerate(store):
        decoded_records += 1
        if not isinstance(record, db.MBOMsg):
            skipped_records += 1
            continue
        row = storage_row(file_sha256, source_ordinal, record)
        mbo_records += 1
        identity = (int(row[5]), int(row[6]))
        identity_rows[identity] = identity_rows.get(identity, 0) + 1
        checksum = add_storage_row_checksum(checksum, row)
        ts_event = int(row[3])
        first_ts_event = ts_event if first_ts_event is None else first_ts_event
        last_ts_event = ts_event
        min_ts_event = ts_event if min_ts_event is None else min(min_ts_event, ts_event)
        max_ts_event = ts_event if max_ts_event is None else max(max_ts_event, ts_event)
        first_source_ordinal = source_ordinal if first_source_ordinal is None else first_source_ordinal
        last_source_ordinal = source_ordinal
        batch.append(row)
        if len(batch) >= batch_size:
            insert_staging_batch(client, staging_table, batch)
            batch_count += 1
    if batch:
        insert_staging_batch(client, staging_table, batch)
        batch_count += 1
    return ContentManifest(
        decoded_records, mbo_records, skipped_records, checksum,
        first_ts_event, last_ts_event, min_ts_event, max_ts_event,
        first_source_ordinal, last_source_ordinal, catalog_identity_rows(identity_rows),
    ), batch_count


def open_mbo_store(path: Path, db: Any) -> Any:
    """打开 DBN，并在真正读取前拒绝非 MBO schema 的文件。"""
    store = db.DBNStore.from_file(path)
    if store.schema is not None and store.schema != db.Schema.MBO:
        raise ValueError(f"{path} 的 DBN schema 为 {store.schema}，仅支持 MBO")
    return store


def insert_staging_batch(client: Any, staging_table: str, rows: list[list[Any]]) -> None:
    """写入后清空同一列表，限制超大历史文件的 Python 常驻内存。"""
    client.insert(staging_table, rows, column_names=STORAGE_COLUMNS)
    rows.clear()


def add_source_stats(stats: ImportStats, manifest: ContentManifest) -> None:
    stats.decoded_records += manifest.decoded_records
    stats.mbo_records += manifest.mbo_records
    stats.skipped_records += manifest.skipped_records


def add_row_checksum(checksum: ContentChecksum, row: Sequence[Any]) -> ContentChecksum:
    return checksum.add(checksum_row_parts(row))


def add_storage_row_checksum(checksum: ContentChecksum, row: Sequence[Any]) -> ContentChecksum:
    return checksum.add(storage_row_checksum_parts(row))


def checksum_row_parts(row: Sequence[Any]) -> tuple[int, int, int, int]:
    # 先按官方二进制范围校验；再使用与 SQL ``toString`` 完全一致的 ASCII 文本
    # 生成摘要，确保 Python 与 ClickHouse 对同一行得到相同内容指纹。
    encode_raw_row(row)
    canonical = "|".join([
        str(int(row[0])),
        str(int(row[1])),
        str(int(row[2])),
        str(int(row[3])),
        str(int(row[4])),
        fixed_char(row[5], "action"),
        fixed_char(row[6], "side"),
        str(int(row[7])),
        str(int(row[8])),
        str(int(row[9])),
        str(int(row[10])),
        str(int(row[11])),
        str(int(row[12])),
        str(int(row[13])),
    ]).encode("ascii")
    return struct.unpack(">QQQQ", hashlib.sha256(canonical).digest())


def storage_row_checksum_parts(row: Sequence[Any]) -> tuple[int, int, int, int]:
    """校验并摘要唯一身份与全部官方字段。"""

    if len(row) != len(STORAGE_COLUMNS):
        raise ValueError(f"MBO 存储记录字段数错误: {len(row)}")
    file_sha256 = str(row[0])
    source_ordinal = int(row[1])
    validate_record_identity(file_sha256, source_ordinal)
    raw = row[len(IDENTITY_COLUMNS):]
    encode_raw_row(raw)
    canonical = "|".join([file_sha256, str(source_ordinal), *canonical_raw_values(raw)]).encode("ascii")
    return struct.unpack(">QQQQ", hashlib.sha256(canonical).digest())


def canonical_raw_values(row: Sequence[Any]) -> list[str]:
    return [
        str(int(row[0])),
        str(int(row[1])),
        str(int(row[2])),
        str(int(row[3])),
        str(int(row[4])),
        fixed_char(row[5], "action"),
        fixed_char(row[6], "side"),
        str(int(row[7])),
        str(int(row[8])),
        str(int(row[9])),
        str(int(row[10])),
        str(int(row[11])),
        str(int(row[12])),
        str(int(row[13])),
    ]


def validate_record_identity(file_sha256: str, source_ordinal: int) -> None:
    if not re.fullmatch(r"[0-9a-f]{64}", file_sha256):
        raise ValueError(f"无效 SHA-256: {file_sha256!r}")
    if source_ordinal < 0 or source_ordinal > (2**64) - 1:
        raise ValueError(f"source_ordinal 超出 UInt64 范围: {source_ordinal}")


def validate_file_order(file_order: int) -> None:
    """确保目录顺序可写入 ClickHouse UInt32，避免跨文件排序出现隐式截断。"""

    if file_order < 0 or file_order > (2**32) - 1:
        raise ValueError("file_order 必须在 UInt32 范围内")


def encode_raw_row(row: Sequence[Any]) -> bytes:
    """验证一行原始数据能否落入 Databento MBO 字段的二进制取值范围。"""
    if len(row) != len(RAW_COLUMNS):
        raise ValueError(f"MBO 原始记录字段数错误: {len(row)}")
    try:
        return RAW_ROW_STRUCT.pack(
            int(row[0]),
            int(row[1]),
            int(row[2]),
            int(row[3]),
            int(row[4]),
            fixed_char(row[5], "action").encode("ascii"),
            fixed_char(row[6], "side").encode("ascii"),
            int(row[7]),
            int(row[8]),
            int(row[9]),
            int(row[10]),
            int(row[11]),
            int(row[12]),
            int(row[13]),
        )
    except (OverflowError, struct.error, UnicodeError, ValueError) as exception:
        raise ValueError(f"MBO 原始记录不符合官方字段类型: {row!r}") from exception


def verify_staged_content(expected: ContentManifest, actual: ContentManifest) -> None:
    """拒绝行数或内容摘要不同的暂存表，防止部分批次被误提交。"""
    if expected.mbo_records != actual.mbo_records or expected.checksum != actual.checksum:
        raise ContentIntegrityError(
            "staging 内容校验失败: "
            f"expected_rows={expected.mbo_records} staged_rows={actual.mbo_records} "
            f"expected_checksum={expected.checksum.display} staged_checksum={actual.checksum.display}"
        )


def read_staging_manifest(client: Any, staging_table: str) -> ContentManifest:
    """在 ClickHouse 端汇总暂存表，避免将全部原始行回传到 Python。"""
    row_hash = storage_checksum_sql()
    chunks = [
        f"reinterpretAsUInt64(reverse(substring(row_hash, {offset}, 8))) AS h{index}"
        for index, offset in enumerate((1, 9, 17, 25))
    ]
    aggregates = ["count()"]
    for index in range(CHECKSUM_PART_COUNT):
        aggregates.extend((f"sumWithOverflow(h{index})", f"groupBitXor(h{index})"))
    query = (
        f"WITH {row_hash} AS row_hash, {', '.join(chunks)} "
        f"SELECT {', '.join(aggregates)} FROM {quote_identifier(staging_table)}"
    )
    result = client.query(query)
    row = result.result_rows[0]
    count = int(row[0])
    sums = tuple(int(row[1 + index * 2]) for index in range(CHECKSUM_PART_COUNT))
    xors = tuple(int(row[2 + index * 2]) for index in range(CHECKSUM_PART_COUNT))
    return ContentManifest(count, count, 0, ContentChecksum(sums, xors))


def raw_checksum_sql() -> str:
    """生成与 ``checksum_row_parts`` 相同字段顺序和分隔符的行摘要 SQL。"""
    arguments: list[str] = []
    for index, column in enumerate(RAW_COLUMNS):
        if index:
            arguments.append("'|'")
        arguments.append(f"toString({quote_identifier(column)})")
    return f"SHA256(concat({', '.join(arguments)}))"


def storage_checksum_sql() -> str:
    """生成覆盖事件身份和官方字段的 ClickHouse 行摘要。"""

    arguments: list[str] = []
    for index, column in enumerate(STORAGE_COLUMNS):
        if index:
            arguments.append("'|'")
        arguments.append(f"toString({quote_identifier(column)})")
    return f"SHA256(concat({', '.join(arguments)}))"


def claim_import_job(
    client: Any,
    path: Path,
    file_sha256: str,
    *,
    worker_id: str,
    lease_seconds: int,
) -> ImportJob | None:
    """原子性有限的 ReplacingMergeTree 上领取一个文件任务。

状态表是追加版本而非原地更新，因此每次读取都使用 ``FINAL``。领取后还会立即
复查 claim_token，缩小两个 worker 同时观察到旧状态时的竞态窗口。
"""
    current = load_job(client, file_sha256)
    if current is not None and current.status == "completed":
        return None

    now = utc_now()
    reject_live_claim(current, now)

    if current is None:
        current = new_pending_job(path, file_sha256)
        save_job(client, current)
        latest = load_job(client, file_sha256)
        if latest is not None:
            current = latest
        if current.status == "completed":
            return None
        reject_live_claim(current, utc_now())

    claimed_at = utc_now()
    claimed = replace(
        current,
        source_path=str(path),
        display_name=path.name,
        file_size=path.stat().st_size,
        status="claimed",
        expected_rows=None,
        staged_rows=0,
        committed_rows=0,
        error_message="",
        attempt=current.attempt + 1,
        claimed_by=worker_id,
        claim_token=uuid4(),
        lease_expires_at=claimed_at + timedelta(seconds=lease_seconds),
        started_at=current.started_at or claimed_at,
        updated_at=claimed_at,
        completed_at=None,
        version=next_job_version(current.version),
    )
    save_job(client, claimed)
    assert_claim(client, file_sha256, claimed.claim_token)
    return claimed


def reject_live_claim(job: ImportJob | None, now: datetime) -> None:
    """租约未到期时拒绝第二个 worker，避免共用同一 SHA 的暂存表。"""
    if (
        job is not None
        and job.status in ACTIVE_JOB_STATUSES
        and job.lease_expires_at is not None
        and ensure_utc(job.lease_expires_at) > now
    ):
        raise ImportInProgressError(
            f"文件正在由 {job.claimed_by or '其他 worker'} 导入，租约到期时间: "
            f"{job.lease_expires_at.isoformat()}"
        )


def new_pending_job(path: Path, file_sha256: str) -> ImportJob:
    now = utc_now()
    return ImportJob(
        file_sha256=file_sha256,
        source_path=str(path),
        display_name=path.name,
        file_size=path.stat().st_size,
        status="pending",
        expected_rows=None,
        staged_rows=0,
        committed_rows=0,
        error_message="",
        attempt=0,
        claimed_by="",
        claim_token=None,
        lease_expires_at=None,
        started_at=None,
        updated_at=now,
        completed_at=None,
        version=next_job_version(0),
    )


def transition_job(
    job: ImportJob,
    status: str,
    *,
    lease_seconds: int,
    **changes: Any,
) -> ImportJob:
    """生成新的不可变状态版本，并为活动阶段续租。"""
    now = utc_now()
    changes.setdefault(
        "lease_expires_at",
        now + timedelta(seconds=lease_seconds) if status in ACTIVE_JOB_STATUSES else None,
    )
    changes.update({
        "status": status,
        "updated_at": now,
        "version": next_job_version(job.version),
    })
    return replace(job, **changes)


def next_job_version(previous: int) -> int:
    """版本同时保持单调递增，并降低同一时钟粒度下版本碰撞的概率。"""
    return max(previous + 1, time.time_ns())


def save_job(client: Any, job: ImportJob) -> None:
    """追加任务状态版本；表引擎按 version 保留当前记录。"""
    client.insert(
        JOB_TABLE,
        [[getattr(job, column) for column in JOB_COLUMNS]],
        column_names=JOB_COLUMNS,
    )


def catalog_entry_from_manifest(
    path: Path,
    file_sha256: str,
    manifest: ContentManifest,
    *,
    updated_at: datetime,
    file_order: int = 0,
    publisher_id: int = 0,
    instrument_id: int = 0,
    mbo_rows: int | None = None,
) -> FileCatalogEntry:
    """从已验证的解码结果生成文件目录行。

    当前默认 ``file_order=0``；需要组合全年或同日分片时，由调用方显式传入，
    再由回放任务按目录排序。
    """

    validate_file_order(file_order)
    path = path.resolve()
    return FileCatalogEntry(
        file_sha256=file_sha256,
        publisher_id=publisher_id,
        instrument_id=instrument_id,
        source_path=str(path),
        display_name=path.name,
        trading_date=trading_date_from_filename(path),
        file_order=file_order,
        file_size=path.stat().st_size,
        decoded_rows=manifest.decoded_records,
        mbo_rows=manifest.mbo_records if mbo_rows is None else mbo_rows,
        skipped_rows=manifest.skipped_records,
        first_ts_event=manifest.first_ts_event,
        last_ts_event=manifest.last_ts_event,
        min_ts_event=manifest.min_ts_event,
        max_ts_event=manifest.max_ts_event,
        first_source_ordinal=manifest.first_source_ordinal,
        last_source_ordinal=manifest.last_source_ordinal,
        status="completed",
        updated_at=updated_at,
        version=next_job_version(0),
    )


def catalog_entries_from_manifest(
    path: Path,
    file_sha256: str,
    manifest: ContentManifest,
    *,
    updated_at: datetime,
    file_order: int = 0,
) -> tuple[FileCatalogEntry, ...]:
    """按文件内 Databento 身份展开目录行，避免将不同合约混为一条回放流。"""

    if not manifest.identity_rows:
        return (
            catalog_entry_from_manifest(
                path,
                file_sha256,
                manifest,
                updated_at=updated_at,
                file_order=file_order,
            ),
        )
    return tuple(
        catalog_entry_from_manifest(
            path,
            file_sha256,
            manifest,
            updated_at=updated_at,
            file_order=file_order,
            publisher_id=publisher_id,
            instrument_id=instrument_id,
            mbo_rows=mbo_rows,
        )
        for publisher_id, instrument_id, mbo_rows in manifest.identity_rows
    )


def trading_date_from_filename(path: Path) -> date | None:
    """仅从标准 DBN 文件名推导交易日，不用事件时间推测。"""

    match = DBN_TRADING_DATE_PATTERN.search(path.name)
    if match is None:
        return None
    try:
        return datetime.strptime(match.group(1), "%Y%m%d").date()
    except ValueError:
        return None


def save_catalog_entry(client: Any, entry: FileCatalogEntry) -> None:
    """目录按文件/身份复合键替换更新；只在原始事件已提交后调用。"""

    client.insert(
        CATALOG_TABLE,
        [[getattr(entry, column) for column in CATALOG_COLUMNS]],
        column_names=CATALOG_COLUMNS,
    )


def catalog_identity_rows(
    identity_rows: dict[tuple[int, int], int],
) -> tuple[tuple[int, int, int], ...]:
    """规范化文件内身份聚合，以便作为单个文件目录行写入 ClickHouse。"""

    return tuple(
        (publisher_id, instrument_id, mbo_rows)
        for (publisher_id, instrument_id), mbo_rows in sorted(identity_rows.items())
    )




def load_job(client: Any, file_sha256: str) -> ImportJob | None:
    """读取单个文件的当前任务状态，必须使用 FINAL 合并历史版本。"""
    result = client.query(
        f"""
        SELECT {', '.join(JOB_COLUMNS)}
        FROM {quote_identifier(JOB_TABLE)} FINAL
        WHERE file_sha256 = toFixedString({{file_sha256:String}}, 64)
        LIMIT 1
        """,
        parameters={"file_sha256": file_sha256},
    )
    if not result.result_rows:
        return None
    return import_job_from_row(result.result_rows[0])


def import_job_from_row(row: Sequence[Any]) -> ImportJob:
    if len(row) != len(JOB_COLUMNS):
        raise RuntimeError(f"dbn_import_jobs 字段数错误: {len(row)}")
    values = dict(zip(JOB_COLUMNS, row, strict=True))
    file_hash = values["file_sha256"]
    if isinstance(file_hash, bytes):
        file_hash = file_hash.rstrip(b"\0").decode("ascii")
    token = values["claim_token"]
    if token is not None and not isinstance(token, UUID):
        token = UUID(str(token))
    return ImportJob(
        file_sha256=str(file_hash),
        source_path=str(values["source_path"]),
        display_name=str(values["display_name"]),
        file_size=int(values["file_size"]),
        status=str(values["status"]),
        expected_rows=None if values["expected_rows"] is None else int(values["expected_rows"]),
        staged_rows=int(values["staged_rows"]),
        committed_rows=int(values["committed_rows"]),
        error_message=str(values["error_message"]),
        attempt=int(values["attempt"]),
        claimed_by=str(values["claimed_by"]),
        claim_token=token,
        lease_expires_at=values["lease_expires_at"],
        started_at=values["started_at"],
        updated_at=values["updated_at"],
        completed_at=values["completed_at"],
        version=int(values["version"]),
    )


def assert_claim(client: Any, file_sha256: str, claim_token: UUID | None) -> ImportJob:
    """确认当前 worker 尚持有领取权；否则中止后续写入。"""
    current = load_job(client, file_sha256)
    if current is None or claim_token is None or current.claim_token != claim_token:
        raise ImportInProgressError(f"文件任务领取权已丢失: {file_sha256}")
    return current


def claim_is_owned(client: Any, file_sha256: str, claim_token: UUID | None) -> bool:
    if claim_token is None:
        return False
    try:
        current = load_job(client, file_sha256)
    except Exception:
        return False
    return current is not None and current.claim_token == claim_token


def staging_table_name(file_sha256: str) -> str:
    if not re.fullmatch(r"[0-9a-f]{64}", file_sha256):
        raise ValueError(f"无效 SHA-256: {file_sha256!r}")
    return f"{STAGING_TABLE_PREFIX}{file_sha256}"


def reset_staging_table(client: Any, staging_table: str) -> None:
    """用正式表定义创建空暂存表，保证字段类型和写入列完全相同。"""
    drop_staging_table(client, staging_table)
    client.command(
        f"CREATE TABLE {quote_identifier(staging_table)} AS {quote_identifier(RAW_TABLE)}"
    )


def drop_staging_table(client: Any, staging_table: str) -> None:
    client.command(f"DROP TABLE IF EXISTS {quote_identifier(staging_table)} SYNC")


def commit_staging_table(
    client: Any,
    staging_table: str,
    *,
    file_sha256: str,
) -> None:
    """将已校验的暂存表一次性转入正式表，并带上文件级去重令牌。

若在 INSERT 成功后、状态标记 completed 前崩溃，重试会使用相同 token；在
ClickHouse 去重窗口内不会再次插入同一个文件块。
"""
    if not re.fullmatch(r"[0-9a-f]{64}", file_sha256):
        raise ValueError(f"无效 SHA-256: {file_sha256!r}")
    columns = storage_column_sql()
    client.command(
        f"INSERT INTO {quote_identifier(RAW_TABLE)} ({columns}) "
        f"SELECT {columns} FROM {quote_identifier(staging_table)}",
        settings={
            "insert_deduplicate": 1,
            "insert_deduplication_token": f"dbn-sha256:{file_sha256}",
        },
    )


def raw_column_sql() -> str:
    return ", ".join(quote_identifier(column) for column in RAW_COLUMNS)


def storage_column_sql() -> str:
    return ", ".join(quote_identifier(column) for column in STORAGE_COLUMNS)


def quote_identifier(identifier: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", identifier):
        raise ValueError(f"无效 ClickHouse 标识符: {identifier!r}")
    return f"`{identifier}`"


def default_worker_id() -> str:
    return os.getenv("DBN_IMPORT_WORKER_ID") or f"{socket.gethostname()}:{os.getpid()}"


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def fixed_char(value: Any, field: str) -> str:
    """读取 DBN 单字符枚举码，不把 A/B 等原始码转换成展示标签。"""

    code = getattr(value, "value", value)
    if isinstance(code, bytes):
        code = code.decode("ascii")
    code = str(code)
    if len(code) != 1:
        raise ValueError(f"DBN MBO {field} 必须是单字符代码: {code!r}")
    return code


def print_summary(stats: ImportStats, *, dry_run: bool) -> None:
    mode = "dry-run" if dry_run else "import"
    print(
        f"{mode}: files={stats.files} completed={stats.completed_files} "
        f"duplicates={stats.duplicate_files} failed={stats.failed_files} "
        f"decoded={stats.decoded_records} "
        f"mbo={stats.mbo_records} inserted={stats.inserted_records} "
        f"staged={stats.staged_records} skipped={stats.skipped_records} "
        f"raw_batches={stats.raw_batches}"
    )


def main(argv: Sequence[str] | None = None) -> int:
    load_dotenv()
    args = parse_args(argv)
    if args.batch_size < 1:
        print("错误: --batch-size 必须是正整数", file=sys.stderr)
        return 2
    if args.max_records < 0:
        print("错误: --max-records 不能小于 0", file=sys.stderr)
        return 2
    if args.file_order < 0 or args.file_order > (2**32) - 1:
        print("错误: --file-order 必须在 UInt32 范围内", file=sys.stderr)
        return 2
    if args.max_records and not args.dry_run:
        print("错误: --max-records 只能与 --dry-run 一起使用", file=sys.stderr)
        return 2

    try:
        paths = expand_input_paths(args.paths)
        db = load_databento()
        client: Any | None = None
        if not args.dry_run:
            client = create_clickhouse_client()
        stats = import_files(
            client,
            paths,
            db=db,
            batch_size=args.batch_size,
            max_records=args.max_records,
            dry_run=args.dry_run,
            file_order=args.file_order,
        )
    except (
        FileNotFoundError,
        RuntimeError,
        ValueError,
        OSError,
    ) as exception:
        print(f"错误: {exception}", file=sys.stderr)
        return 1

    print_summary(stats, dry_run=args.dry_run)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
