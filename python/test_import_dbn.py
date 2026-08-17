from __future__ import annotations

import hashlib
import re
import sys
import tempfile
import unittest
from collections.abc import Sequence
from datetime import date
from pathlib import Path
from types import SimpleNamespace
from typing import Any

sys.path.insert(0, str(Path(__file__).parent))

import import_dbn


class FakeMBO:
    def __init__(
        self,
        instrument_id: int = 750,
        *,
        flags: int = 0,
        price: int = 18_000_250_000_000,
        sequence: int = 12,
    ) -> None:
        self.rtype = 160
        self.publisher_id = 1
        self.instrument_id = instrument_id
        self.ts_event = 1_700_000_000_123_456_789 + sequence
        self.ts_recv = 1_700_000_000_123_456_999 + sequence
        self.sequence = sequence
        self.channel_id = 7
        self.ts_in_delta = 210
        self.action = "A"
        self.side = "B"
        self.order_id = 999 + sequence
        self.price = price
        self.size = 3
        self.flags = flags


class FakeOtherRecord:
    pass


class FakeStore(list):
    def __init__(self, records: list[object]) -> None:
        super().__init__(records)
        self.schema = "mbo"


class FakeClient:
    def __init__(
        self,
        *,
        corrupt_staging_read: bool = False,
        fail_after_commit_once: bool = False,
        interrupt_staging_once: bool = False,
    ) -> None:
        self.job_versions: dict[str, list[dict[str, Any]]] = {}
        self.staging_tables: dict[str, list[list[Any]]] = {}
        self.raw_rows: list[list[Any]] = []
        self.catalog_rows: list[dict[str, Any]] = []
        self.commands: list[str] = []
        self.corrupt_staging_read = corrupt_staging_read
        self.fail_after_commit_once = fail_after_commit_once
        self.interrupt_staging_once = interrupt_staging_once
        self.committed_tokens: set[str] = set()

    def insert(
        self,
        table: str,
        rows: list[list[Any]],
        *,
        column_names: Sequence[str],
    ) -> None:
        if table == import_dbn.JOB_TABLE:
            for row in rows:
                values = dict(zip(column_names, row, strict=True))
                self.job_versions.setdefault(str(values["file_sha256"]), []).append(values)
            return
        if table == import_dbn.CATALOG_TABLE:
            for row in rows:
                self.catalog_rows.append(dict(zip(column_names, row, strict=True)))
            return
        if table.startswith(import_dbn.STAGING_TABLE_PREFIX):
            if tuple(column_names) != import_dbn.STORAGE_COLUMNS:
                raise AssertionError("staging 列缺少事件身份或官方 14 字段")
            if self.interrupt_staging_once:
                self.interrupt_staging_once = False
                raise KeyboardInterrupt
            self.staging_tables[table].extend([list(row) for row in rows])
            return
        raise AssertionError(f"unexpected insert table: {table}")

    def query(self, query: str, parameters: dict[str, Any] | None = None) -> SimpleNamespace:
        if f"`{import_dbn.JOB_TABLE}` FINAL" in query:
            file_sha256 = str((parameters or {})["file_sha256"])
            current = self.current_job(file_sha256)
            rows = [] if current is None else [tuple(current[column] for column in import_dbn.JOB_COLUMNS)]
            return SimpleNamespace(result_rows=rows)

        match = re.search(r"FROM `([^`]+)`", query)
        if match is None:
            raise AssertionError(f"unexpected query: {query}")
        table = match.group(1)
        rows = [list(row) for row in self.staging_tables[table]]
        if self.corrupt_staging_read and rows:
            rows[0][import_dbn.STORAGE_COLUMNS.index("price")] += 1
        if "sumWithOverflow" in query:
            checksum = import_dbn.ContentChecksum.empty()
            for row in rows:
                checksum = import_dbn.add_storage_row_checksum(checksum, row)
            result = [len(rows)]
            for row_sum, row_xor in zip(checksum.sums, checksum.xors, strict=True):
                result.extend((row_sum, row_xor))
            return SimpleNamespace(result_rows=[tuple(result)])
        return SimpleNamespace(result_rows=rows)

    def command(
        self,
        command: str,
        *,
        settings: dict[str, Any] | None = None,
    ) -> None:
        self.commands.append(command)
        identifiers = re.findall(r"`([^`]+)`", command)
        if command.startswith("DROP TABLE IF EXISTS"):
            self.staging_tables.pop(identifiers[0], None)
            return
        if command.startswith("CREATE TABLE"):
            staging_table = identifiers[0]
            if staging_table in self.staging_tables:
                raise RuntimeError(f"table already exists: {staging_table}")
            self.staging_tables[staging_table] = []
            return
        if command.startswith("INSERT INTO"):
            match = re.search(r"FROM `([^`]+)`", command)
            if match is None or identifiers[0] != import_dbn.RAW_TABLE:
                raise AssertionError(f"unexpected commit command: {command}")
            token = str((settings or {}).get("insert_deduplication_token", ""))
            if not token:
                raise AssertionError("commit 缺少 insert_deduplication_token")
            if token not in self.committed_tokens:
                self.raw_rows.extend([list(row) for row in self.staging_tables[match.group(1)]])
                self.committed_tokens.add(token)
            if self.fail_after_commit_once:
                self.fail_after_commit_once = False
                raise RuntimeError("模拟服务端已提交但客户端未收到确认")
            return
        raise AssertionError(f"unexpected command: {command}")

    def current_job(self, file_sha256: str) -> dict[str, Any] | None:
        versions = self.job_versions.get(file_sha256, [])
        return max(versions, key=lambda row: int(row["version"])) if versions else None

    def statuses(self, file_sha256: str) -> list[str]:
        return [str(row["status"]) for row in self.job_versions.get(file_sha256, [])]


def fake_db(stores: dict[str, FakeStore]) -> SimpleNamespace:
    open_counts: dict[str, int] = {}

    def from_file(path: Path) -> FakeStore:
        open_counts[path.name] = open_counts.get(path.name, 0) + 1
        return stores[path.name]

    return SimpleNamespace(
        DBNStore=SimpleNamespace(from_file=from_file),
        MBOMsg=FakeMBO,
        Schema=SimpleNamespace(MBO="mbo"),
        open_counts=open_counts,
    )


class ImportDbnTests(unittest.TestCase):
    def test_raw_row_preserves_official_mbo_fields(self) -> None:
        record = FakeMBO(750, flags=40, sequence=12)

        self.assertEqual(
            import_dbn.RAW_COLUMNS,
            (
                "ts_recv", "ts_event", "rtype", "publisher_id", "instrument_id", "action", "side",
                "price", "size", "channel_id", "order_id", "flags", "ts_in_delta", "sequence",
            ),
        )
        row = import_dbn.raw_row(record)
        self.assertEqual(row[import_dbn.RAW_COLUMNS.index("instrument_id")], 750)
        self.assertEqual(row[import_dbn.RAW_COLUMNS.index("action")], "A")
        self.assertEqual(row[import_dbn.RAW_COLUMNS.index("flags")], 40)
        self.assertNotIn("record_index", import_dbn.RAW_COLUMNS)
        self.assertNotIn("instrument", import_dbn.RAW_COLUMNS)
        self.assertEqual(import_dbn.STORAGE_COLUMNS[:2], ("file_sha256", "source_ordinal"))
        self.assertEqual(len(import_dbn.encode_raw_row(row)), import_dbn.RAW_ROW_STRUCT.size)
        self.assertIn("SHA256", import_dbn.raw_checksum_sql())
        self.assertIn("source_ordinal", import_dbn.storage_checksum_sql())

    def test_full_import_stages_verifies_and_commits_once(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "nq.dbn"
            file_bytes = b"fake DBN content v1"
            path.write_bytes(file_bytes)
            records = [FakeMBO(750, sequence=1), FakeOtherRecord(), FakeMBO(999_999, sequence=2)]
            client = FakeClient()

            stats = import_dbn.import_file(
                client,
                path,
                db=fake_db({path.name: FakeStore(records)}),
                batch_size=1,
                worker_id="test-worker",
            )

            file_sha256 = hashlib.sha256(file_bytes).hexdigest()
            job = client.current_job(file_sha256)
            self.assertIsNotNone(job)
            self.assertEqual(job["status"], "completed")
            self.assertEqual(job["expected_rows"], 2)
            self.assertEqual(job["staged_rows"], 2)
            self.assertEqual(job["committed_rows"], 2)
            self.assertEqual(job["attempt"], 1)
            self.assertEqual(
                client.statuses(file_sha256),
                ["pending", "claimed", "staging", "staging", "committing", "completed"],
            )
            self.assertEqual(client.raw_rows, [
                import_dbn.storage_row(file_sha256, 0, records[0]),
                import_dbn.storage_row(file_sha256, 2, records[2]),
            ])
            self.assertEqual(len(client.catalog_rows), 2)
            self.assertEqual(
                [
                    (row["publisher_id"], row["instrument_id"], row["mbo_rows"])
                    for row in client.catalog_rows
                ],
                [(1, 750, 1), (1, 999_999, 1)],
            )
            catalog = client.catalog_rows[0]
            self.assertEqual(catalog["file_sha256"], file_sha256)
            self.assertIsNone(catalog["trading_date"])
            self.assertEqual(catalog["decoded_rows"], 3)
            self.assertEqual(catalog["mbo_rows"], 1)
            self.assertEqual(catalog["skipped_rows"], 1)
            self.assertEqual(catalog["first_source_ordinal"], 0)
            self.assertEqual(catalog["last_source_ordinal"], 2)
            self.assertEqual(catalog["first_ts_event"], records[0].ts_event)
            self.assertEqual(catalog["last_ts_event"], records[2].ts_event)
            self.assertEqual(catalog["min_ts_event"], min(records[0].ts_event, records[2].ts_event))
            self.assertEqual(catalog["max_ts_event"], max(records[0].ts_event, records[2].ts_event))
            self.assertEqual(catalog["file_order"], 0)
            self.assertEqual(client.staging_tables, {})
            self.assertEqual(sum(command.startswith("INSERT INTO") for command in client.commands), 1)
            self.assertEqual(stats.completed_files, 1)
            self.assertEqual(stats.staged_records, 2)
            self.assertEqual(stats.inserted_records, 2)
            self.assertEqual(stats.raw_batches, 2)

    def test_completed_sha256_skips_same_content_at_another_path(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            first = Path(temp_dir) / "first.dbn"
            second = Path(temp_dir) / "second.dbn"
            first.write_bytes(b"identical bytes")
            second.write_bytes(b"identical bytes")
            db = fake_db({
                first.name: FakeStore([FakeMBO(750)]),
                second.name: FakeStore([FakeMBO(999_999)]),
            })
            client = FakeClient()

            stats = import_dbn.import_files(client, [first, second], db=db, batch_size=10)

            self.assertEqual(stats.completed_files, 1)
            self.assertEqual(stats.duplicate_files, 1)
            self.assertEqual(stats.inserted_records, 1)
            self.assertEqual(len(client.raw_rows), 1)
            self.assertEqual(db.open_counts.get(first.name), 1)
            self.assertNotIn(second.name, db.open_counts)

    def test_checksum_mismatch_marks_failed_without_committing_and_can_retry(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "nq.dbn"
            file_bytes = b"retryable DBN"
            path.write_bytes(file_bytes)
            stores = {path.name: FakeStore([FakeMBO(750), FakeMBO(13743)])}
            client = FakeClient(corrupt_staging_read=True)
            failed_stats = import_dbn.ImportStats(files=1)

            with self.assertRaises(import_dbn.ContentIntegrityError):
                import_dbn.import_file(
                    client,
                    path,
                    db=fake_db(stores),
                    batch_size=10,
                    stats=failed_stats,
                    worker_id="test-worker",
                )

            file_sha256 = hashlib.sha256(file_bytes).hexdigest()
            failed_job = client.current_job(file_sha256)
            self.assertEqual(failed_job["status"], "failed")
            self.assertIn("staging 内容校验失败", failed_job["error_message"])
            self.assertEqual(client.raw_rows, [])
            self.assertEqual(client.staging_tables, {})
            self.assertEqual(sum(command.startswith("INSERT INTO") for command in client.commands), 0)
            self.assertEqual(failed_stats.failed_files, 1)

            client.corrupt_staging_read = False
            retry_stats = import_dbn.import_file(
                client,
                path,
                db=fake_db(stores),
                batch_size=10,
                worker_id="test-worker",
            )

            completed_job = client.current_job(file_sha256)
            self.assertEqual(completed_job["status"], "completed")
            self.assertEqual(completed_job["attempt"], 2)
            self.assertEqual(retry_stats.inserted_records, 2)
            self.assertEqual(len(client.raw_rows), 2)

    def test_retry_after_lost_commit_ack_uses_same_file_token(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "nq.dbn"
            file_bytes = b"commit acknowledgement can be lost"
            path.write_bytes(file_bytes)
            records = [FakeMBO(750), FakeMBO(13743)]
            stores = {path.name: FakeStore(records)}
            client = FakeClient(fail_after_commit_once=True)

            with self.assertRaisesRegex(RuntimeError, "客户端未收到确认"):
                import_dbn.import_file(
                    client,
                    path,
                    db=fake_db(stores),
                    batch_size=10,
                    worker_id="test-worker",
                )

            file_sha256 = hashlib.sha256(file_bytes).hexdigest()
            self.assertEqual(client.current_job(file_sha256)["status"], "failed")
            self.assertEqual(len(client.raw_rows), 2)

            retry_stats = import_dbn.import_file(
                client,
                path,
                db=fake_db(stores),
                batch_size=10,
                worker_id="test-worker",
            )

            self.assertEqual(client.current_job(file_sha256)["status"], "completed")
            self.assertEqual(retry_stats.inserted_records, 2)
            self.assertEqual(len(client.raw_rows), 2)
            self.assertEqual(
                client.committed_tokens,
                {f"dbn-sha256:{file_sha256}"},
            )

    def test_keyboard_interrupt_marks_failed_and_cleans_staging(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "nq.dbn"
            file_bytes = b"interruptible DBN"
            path.write_bytes(file_bytes)
            client = FakeClient(interrupt_staging_once=True)

            with self.assertRaises(KeyboardInterrupt):
                import_dbn.import_file(
                    client,
                    path,
                    db=fake_db({path.name: FakeStore([FakeMBO(750)])}),
                    batch_size=10,
                    worker_id="test-worker",
                )

            file_sha256 = hashlib.sha256(file_bytes).hexdigest()
            job = client.current_job(file_sha256)
            self.assertEqual(job["status"], "failed")
            self.assertEqual(job["error_message"], "导入被用户中断")
            self.assertEqual(client.staging_tables, {})
            self.assertEqual(client.raw_rows, [])

    def test_max_records_is_dry_run_only(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "nq.dbn"
            path.write_bytes(b"partial scan only")
            db = fake_db({path.name: FakeStore([FakeMBO(750), FakeMBO(13743)])})

            with self.assertRaisesRegex(ValueError, "只能与 --dry-run"):
                import_dbn.import_file(FakeClient(), path, db=db, batch_size=10, max_records=1)

            stats = import_dbn.import_file(
                None,
                path,
                db=db,
                batch_size=10,
                max_records=1,
                dry_run=True,
            )
            self.assertEqual(stats.decoded_records, 1)
            self.assertEqual(stats.mbo_records, 1)
            self.assertEqual(stats.inserted_records, 0)

    def test_unexpired_claim_is_not_stolen(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "nq.dbn"
            path.write_bytes(b"claimed DBN")
            file_sha256 = import_dbn.calculate_file_sha256(path)
            client = FakeClient()
            db = fake_db({path.name: FakeStore([FakeMBO(750)])})
            claimed = import_dbn.claim_import_job(
                client,
                path,
                file_sha256,
                worker_id="first-worker",
                lease_seconds=60,
            )

            self.assertIsNotNone(claimed)
            with self.assertRaises(import_dbn.ImportInProgressError):
                import_dbn.import_file(
                    client,
                    path,
                    db=db,
                    batch_size=10,
                    worker_id="second-worker",
                )
            self.assertEqual(client.current_job(file_sha256)["attempt"], 1)
            self.assertEqual(client.raw_rows, [])

    def test_undefined_price_is_kept_as_its_native_value(self) -> None:
        undefined_price = (2**63) - 1
        row = import_dbn.raw_row(FakeMBO(price=undefined_price))

        self.assertEqual(row[import_dbn.RAW_COLUMNS.index("price")], undefined_price)

    def test_catalog_trading_date_uses_only_standard_dbn_filename(self) -> None:
        self.assertEqual(
            import_dbn.trading_date_from_filename(Path("glbx-mdp3-20240104.mbo.dbn.zst")),
            date(2024, 1, 4),
        )
        self.assertIsNone(import_dbn.trading_date_from_filename(Path("NQ_capture_001.mbo.dbn.zst")))

    def test_catalog_keeps_explicit_file_order(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "glbx-mdp3-20240104.mbo.dbn.zst"
            path.write_bytes(b"catalog fixture")
            entry = import_dbn.catalog_entry_from_manifest(
                path,
                "a" * 64,
                import_dbn.ContentManifest(1, 1, 0, import_dbn.ContentChecksum.empty()),
                updated_at=import_dbn.utc_now(),
                file_order=3,
            )

        self.assertEqual(entry.file_order, 3)

    def test_catalog_entries_expand_each_identity(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "catalog.dbn"
            path.write_bytes(b"catalog fixture")
            entries = import_dbn.catalog_entries_from_manifest(
                path,
                "a" * 64,
                import_dbn.ContentManifest(
                    3,
                    2,
                    1,
                    import_dbn.ContentChecksum.empty(),
                    identity_rows=((1, 750, 1), (2, 42, 1)),
                ),
                updated_at=import_dbn.utc_now(),
            )

        self.assertEqual(
            [(entry.publisher_id, entry.instrument_id, entry.mbo_rows) for entry in entries],
            [(1, 750, 1), (2, 42, 1)],
        )

    def test_catalog_rejects_file_order_outside_uint32(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "glbx-mdp3-20240104.mbo.dbn.zst"
            path.write_bytes(b"catalog fixture")
            with self.assertRaisesRegex(ValueError, "file_order"):
                import_dbn.catalog_entry_from_manifest(
                    path,
                    "a" * 64,
                    import_dbn.ContentManifest(1, 1, 0, import_dbn.ContentChecksum.empty()),
                    updated_at=import_dbn.utc_now(),
                    file_order=-1,
                )


if __name__ == "__main__":
    unittest.main()
