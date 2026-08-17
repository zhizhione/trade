from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

import kafka_consumer


class FakeClickHouseClient:
    def __init__(self) -> None:
        self.inserts: list[tuple[str, list[list[object]], list[str]]] = []

    def insert(self, table: str, rows: list[list[object]], *, column_names: list[str]) -> None:
        self.inserts.append((table, [list(row) for row in rows], list(column_names)))


class KafkaConsumerDatabentoRowTests(unittest.TestCase):
    @staticmethod
    def native_mbo_source() -> dict[str, int | str]:
        return {
            "file_sha256": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            "source_ordinal": 42,
            "ts_recv": 1_704_153_600_000_000_000,
            "ts_event": 1_704_027_604_375_472_715,
            "rtype": 160,
            "publisher_id": 1,
            "instrument_id": 750,
            "action": "A",
            "side": "B",
            "price": 17_000_750_000_000,
            "size": 1,
            "channel_id": 8,
            "order_id": 6849026235350,
            "flags": 40,
            "ts_in_delta": 0,
            "sequence": 2305,
        }

    def test_databento_row_preserves_official_native_fields(self) -> None:
        source = self.native_mbo_source()

        event = kafka_consumer.normalize("market.mbo", source)
        row = kafka_consumer.databento_mbo_row(source, event)

        self.assertEqual(tuple(row), kafka_consumer.DATABENTO_MBO_COLUMNS)
        self.assertEqual(row, source)
        self.assertEqual(event["event_time"].isoformat(), "2023-12-31T13:00:04.375472+00:00")
        self.assertNotIn("record_index", row)
        self.assertNotIn("instrument", row)
        self.assertNotIn("dataset", row)
        self.assertEqual(row["source_ordinal"], 42)
        self.assertEqual(row["sequence"], 2305)
        self.assertEqual(row["flags"], 40)

    def test_clickhouse_write_uses_official_column_order(self) -> None:
        source = self.native_mbo_source()
        client = FakeClickHouseClient()

        kafka_consumer.write_clickhouse(client, kafka_consumer.normalize("market.mbo", source))

        self.assertEqual(len(client.inserts), 1)
        table, rows, column_names = client.inserts[0]
        self.assertEqual(table, "databento_mbo_raw")
        self.assertEqual(column_names, list(kafka_consumer.DATABENTO_MBO_COLUMNS))
        self.assertEqual(rows, [[source[column] for column in kafka_consumer.DATABENTO_MBO_COLUMNS]])


if __name__ == "__main__":
    unittest.main()
