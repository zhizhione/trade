#!/usr/bin/env python3
"""消费实时 Kafka 行情，并按来源分别写入原始表和标准化派生表。

消费者保留上游携带的来源身份。Databento MBO 原始表保存稳定事件身份、顺序和官方
14 字段，ATAS 原始表和成交派生表则保留各自的实时语义。
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import signal
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import clickhouse_connect
from dotenv import load_dotenv
from kafka import KafkaConsumer

DEFAULT_TOPICS = (
    "market.tick",
    "market.trade",
    "market.order_book",
    "market.mbo",
    "market.signal",
)
# 文件身份与序号保证可唯一定位和回放；其余列与 Databento 官方 MBO 记录一致。
DATABENTO_MBO_COLUMNS = (
    "file_sha256",
    "source_ordinal",
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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    parser.add_argument("--group-id", default="market-data-offline")
    parser.add_argument("--topics", nargs="+", default=list(DEFAULT_TOPICS))
    parser.add_argument("--csv", type=Path, help="将标准化事件追加到此 CSV 文件")
    parser.add_argument("--clickhouse", action="store_true", help="同时写入 ClickHouse")
    parser.add_argument("--max-messages", type=int, default=0, help="处理 N 条事件后停止；0 表示不限制")
    return parser.parse_args()


def text(source: dict[str, Any], *keys: str) -> str | None:
    for key in keys:
        value = source.get(key)
        if value is not None and str(value).strip():
            return str(value).strip()
    return None


def number(source: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        value = source.get(key)
        if value is not None and value != "":
            return value
    return None


def integer(source: dict[str, Any], *keys: str, default: int | None = None) -> int | None:
    value = number(source, *keys)
    if value is None:
        return default
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return parsed


def decimal(source: dict[str, Any], *keys: str) -> Decimal | None:
    value = number(source, *keys)
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def source_type(source: dict[str, Any]) -> str | None:
    declared = (text(source, "source", "feed", "provider", "vendor") or "").lower()
    if "atas" in declared:
        return "atas"
    if "databento" in declared:
        return "databento"
    if text(source, "source_stream_id", "sourceStreamId", "stream_id", "streamId"):
        return "atas"
    if number(source, "event_time_kind", "eventTimeKind") is not None:
        return "atas"
    if number(source, "dataset", "publisher_id", "publisherId", "instrument_id", "instrumentId") is not None:
        return "databento"
    return None


def parse_time(value: Any, *, local_zone: ZoneInfo | None = None) -> datetime:
    """将秒/毫秒/ISO 时间统一为 UTC；无时区文本仅按来源约定的本地时区解释。"""
    if value is None:
        return datetime.now(timezone.utc)
    if isinstance(value, (int, float)):
        seconds = value / 1_000 if abs(value) >= 10_000_000_000 else value
        return datetime.fromtimestamp(seconds, timezone.utc)
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00").replace(" ", "T"))
    except ValueError as exception:
        raise ValueError(f"invalid market event timestamp: {value}") from exception
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=local_zone or timezone.utc)
    return parsed.astimezone(timezone.utc)


def parse_databento_timestamp(value: Any) -> datetime:
    """将 Databento 原始纳秒时间戳用于消费者的通用事件时间。"""

    try:
        timestamp_ns = int(value)
    except (TypeError, ValueError) as exception:
        raise ValueError(f"invalid Databento nanosecond timestamp: {value}") from exception
    seconds, nanoseconds = divmod(timestamp_ns, 1_000_000_000)
    return datetime.fromtimestamp(seconds, timezone.utc) + timedelta(microseconds=nanoseconds // 1_000)


def normalize(topic: str, source: dict[str, Any]) -> dict[str, Any]:
    """生成展示、CSV 和派生写入共用的事件外壳，不改变原始 payload。

Databento 的 ``ts_event`` 是纳秒整数，Python ``datetime`` 只能保留到微秒；
因此该时间仅供通用事件展示，写入 Databento 原始表时仍使用未转换的整数值。
"""
    feed = source_type(source)
    event_type = text(source, "eventType", "event_type", "dataType", "data_type", "messageType", "message_type")
    candidate_type = text(source, "type")
    if event_type is None and candidate_type and not (
        feed == "atas" and candidate_type.lower() in {"new", "change", "delete"}
    ):
        event_type = candidate_type
    event_type = event_type or topic.rsplit(".", 1)[-1]
    symbol = (text(source, "contract_symbol", "contractSymbol", "symbol", "instrument", "ticker") or "UNKNOWN").upper()
    raw_event_time = number(source, "ts_event", "eventTime", "event_time", "timestamp", "ts")
    if feed == "databento" and raw_event_time is not None:
        event_time = parse_databento_timestamp(raw_event_time)
    else:
        event_time = parse_time(
            raw_event_time,
            local_zone=ZoneInfo(os.getenv("ATAS_EVENT_TIME_ZONE", "America/Chicago")) if feed == "atas" else None,
        )
    sequence = integer(source, "source_sequence", "sourceSequence", "sequence", "seq")
    source_stream = text(source, "source_stream_id", "sourceStreamId", "stream_id", "streamId")
    event_id = text(source, "source_event_id", "sourceEventId", "event_id", "eventId", "id")
    if event_id is None and source_stream and sequence is not None:
        event_id = f"{feed or 'market'}:{source_stream}:{sequence}"
    event_id = event_id or f"offline-{event_time.timestamp()}"
    return {
        "event_id": event_id,
        "event_time": event_time,
        "received_at": datetime.now(timezone.utc),
        "topic": topic,
        "event_type": event_type.lower(),
        "symbol": symbol,
        "sequence": sequence,
        "price": number(source, "price", "lastPrice", "last_price"),
        "quantity": number(source, "quantity", "qty", "size", "volume"),
        "side": (text(source, "side", "direction", "aggressorSide", "aggressor_side") or "").upper(),
        "payload": json.dumps(source, separators=(",", ":"), ensure_ascii=False),
    }


def clickhouse_client():
    return clickhouse_connect.get_client(
        host=os.getenv("CLICKHOUSE_HOST", "localhost"),
        port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
        username=os.getenv("CLICKHOUSE_USERNAME", "market"),
        password=os.getenv("CLICKHOUSE_PASSWORD", ""),
        database=os.getenv("CLICKHOUSE_DATABASE", "market_data"),
    )


def write_clickhouse(client: Any, event: dict[str, Any]) -> None:
    """按来源和事件类型写入对应表，避免把不同数据源的字段语义混在一起。"""
    source = json.loads(event["payload"])
    feed = source_type(source)
    if feed == "atas" and event["event_type"] == "mbo":
        row = atas_mbo_row(source, event)
        client.insert("atas_mbo_raw", [list(row.values())], column_names=list(row))
        return
    if feed == "atas" and event["event_type"] == "trade":
        row = atas_trade_row(source, event)
        client.insert("atas_trade_raw", [list(row.values())], column_names=list(row))
        trade = normalized_trade_row(source, event, "atas")
        client.insert("trades", [list(trade.values())], column_names=list(trade))
        return
    if feed == "databento" and event["event_type"] == "mbo":
        row = databento_mbo_row(source, event)
        client.insert(
            "databento_mbo_raw",
            [[row[column] for column in DATABENTO_MBO_COLUMNS]],
            column_names=list(DATABENTO_MBO_COLUMNS),
        )
        if str(number(source, "action") or "").lower() in {"trade", "fill", "t", "f"}:
            trade = normalized_trade_row(source, event, "databento")
            client.insert("trades", [list(trade.values())], column_names=list(trade))
        return
    raise ValueError(f"unsupported ClickHouse event source/type: {feed}/{event['event_type']}")


def require(source: dict[str, Any], *keys: str) -> Any:
    value = number(source, *keys)
    if value is None:
        raise ValueError(f"missing required event field: {keys[0]}")
    return value


def uuid_value(source: dict[str, Any]) -> str:
    value = str(require(source, "source_stream_id", "sourceStreamId", "stream_id", "streamId"))
    import uuid
    uuid.UUID(value)
    return value


def price_nano(source: dict[str, Any], price: Decimal | None) -> int | None:
    supplied = integer(source, "price_nano", "priceNano")
    if supplied is not None:
        return supplied
    if price is None:
        return None
    return int(price * Decimal("1000000000"))


def received_time(source: dict[str, Any], event: dict[str, Any]) -> datetime:
    raw = number(source, "received_utc", "receivedUtc", "received_at", "receivedAt", "ts_recv_raw", "ts_recv")
    return parse_time(raw) if raw is not None else event["received_at"]


def event_time_raw(source: dict[str, Any], event: dict[str, Any]) -> str:
    return str(number(source, "event_time", "eventTime", "timestamp", "ts") or event["event_time"].isoformat())


def event_time_kind(source: dict[str, Any]) -> str:
    kind = text(source, "event_time_kind", "eventTimeKind")
    if kind:
        return kind
    raw = event_time_raw(source, {"event_time": datetime.now(timezone.utc)})
    return "Utc" if raw.endswith("Z") or "+" in raw[-7:] or "-" in raw[-7:] else "Unspecified"


def canonical_id(source: dict[str, Any]) -> int:
    value = integer(source, "canonical_id", "canonicalId")
    if value is None or value < 0:
        raise ValueError("missing required event field: canonical_id")
    return value


def sequence(source: dict[str, Any], event: dict[str, Any]) -> int:
    value = integer(source, "source_sequence", "sourceSequence", "sequence", "seq")
    if value is None or value < 0:
        value = event.get("sequence")
    if value is None or int(value) < 0:
        raise ValueError("missing required event field: source_sequence")
    return int(value)


def side_name(source: dict[str, Any]) -> str:
    value = (text(source, "direction", "aggressor_side", "aggressorSide", "side") or "").upper()
    return {"BUY": "Buy", "B": "Buy", "SELL": "Sell", "S": "Sell"}.get(value, "Unknown")


def base_source_fields(source: dict[str, Any], event: dict[str, Any]) -> tuple[int, int, Decimal, int]:
    price = decimal(source, "price", "px")
    volume = integer(source, "volume", "quantity", "qty", "size")
    if price is None or volume is None or volume < 0:
        raise ValueError("price and non-negative volume are required")
    return canonical_id(source), sequence(source, event), price, volume


def atas_mbo_row(source: dict[str, Any], event: dict[str, Any]) -> dict[str, Any]:
    canonical, seq, price, volume = base_source_fields(source, event)
    return {
        "schema_version": integer(source, "schema_version", "schemaVersion", default=1),
        "source_stream_id": uuid_value(source),
        "source_sequence": seq,
        "received_utc": received_time(source, event),
        "event_time_utc": event["event_time"],
        "event_time_raw": event_time_raw(source, event),
        "event_time_kind": event_time_kind(source),
        "canonical_id": canonical,
        "root_symbol": text(source, "root_symbol", "rootSymbol", "root") or event["symbol"],
        "contract_symbol": text(source, "contract_symbol", "contractSymbol", "contract") or event["symbol"],
        "exchange": text(source, "exchange", "venue") or "UNKNOWN",
        "update_type": text(source, "update_type", "updateType", "action") or "Unknown",
        "side": text(source, "side") or "Unknown",
        "priority": integer(source, "priority", default=0),
        "exchange_order_id": integer(source, "exchange_order_id", "exchangeOrderId", "order_id", "orderId", default=0),
        "price": price,
        "price_nano": price_nano(source, price),
        "volume": volume,
    }


def atas_trade_row(source: dict[str, Any], event: dict[str, Any]) -> dict[str, Any]:
    canonical, seq, price, volume = base_source_fields(source, event)
    return {
        "schema_version": integer(source, "schema_version", "schemaVersion", default=1),
        "source_stream_id": uuid_value(source),
        "source_sequence": seq,
        "received_utc": received_time(source, event),
        "event_time_utc": event["event_time"],
        "event_time_raw": event_time_raw(source, event),
        "event_time_kind": event_time_kind(source),
        "canonical_id": canonical,
        "root_symbol": text(source, "root_symbol", "rootSymbol", "root") or event["symbol"],
        "contract_symbol": text(source, "contract_symbol", "contractSymbol", "contract") or event["symbol"],
        "exchange": text(source, "exchange", "venue") or "UNKNOWN",
        "direction": side_name(source),
        "data_type": text(source, "data_type", "dataType") or "Trade",
        "price": price,
        "price_nano": price_nano(source, price),
        "volume": volume,
        "passive_exchange_order_id": integer(source, "passive_exchange_order_id", "passiveExchangeOrderId", default=0),
        "aggressor_exchange_order_id": integer(source, "aggressor_exchange_order_id", "aggressorExchangeOrderId", default=0),
    }


def databento_mbo_row(source: dict[str, Any], _event: dict[str, Any]) -> dict[str, Any]:
    """返回稳定事件身份和官方 MBO 原始字段，不猜测缺失的文件顺序。"""

    values = (
        required_file_sha256(source),
        required_mbo_integer(source, "source_ordinal"),
        required_mbo_integer(source, "ts_recv"),
        required_mbo_integer(source, "ts_event"),
        required_mbo_integer(source, "rtype"),
        required_mbo_integer(source, "publisher_id"),
        required_mbo_integer(source, "instrument_id"),
        required_mbo_code(source, "action"),
        required_mbo_code(source, "side"),
        required_mbo_integer(source, "price"),
        required_mbo_integer(source, "size"),
        required_mbo_integer(source, "channel_id"),
        required_mbo_integer(source, "order_id"),
        required_mbo_integer(source, "flags"),
        required_mbo_integer(source, "ts_in_delta"),
        required_mbo_integer(source, "sequence"),
    )
    return dict(zip(DATABENTO_MBO_COLUMNS, values, strict=True))


def required_file_sha256(source: dict[str, Any]) -> str:
    value = str(source.get("file_sha256", ""))
    if not re.fullmatch(r"[0-9a-f]{64}", value):
        raise ValueError(f"invalid Databento MBO file_sha256: {value!r}")
    return value


def required_mbo_integer(source: dict[str, Any], field: str) -> int:
    value = source.get(field)
    if value is None or value == "":
        raise ValueError(f"missing required Databento MBO field: {field}")
    try:
        return int(value)
    except (TypeError, ValueError) as exception:
        raise ValueError(f"invalid Databento MBO integer field {field}: {value!r}") from exception


def required_mbo_code(source: dict[str, Any], field: str) -> str:
    value = source.get(field)
    if value is None:
        raise ValueError(f"missing required Databento MBO field: {field}")
    if isinstance(value, bytes):
        value = value.decode("ascii")
    code = str(value)
    if len(code) != 1:
        raise ValueError(f"Databento MBO {field} must be one character: {code!r}")
    return code


def normalized_trade_row(source: dict[str, Any], event: dict[str, Any], feed: str) -> dict[str, Any]:
    """构造跨来源统一成交行；事件身份优先使用来源 ID，其次使用流和序列。"""
    canonical, seq, price, volume = base_source_fields(source, event)
    stream_id = text(source, "source_stream_id", "sourceStreamId", "stream_id", "streamId")
    source_event_id = text(source, "source_event_id", "sourceEventId", "event_id", "eventId", "id")
    source_event_id = source_event_id or (f"{feed}:{stream_id}:{seq}" if stream_id else f"{feed}:{event['topic']}:{seq}")
    return {
        "source": feed,
        "source_event_id": source_event_id,
        "source_stream_id": stream_id,
        "source_sequence": seq,
        "canonical_id": canonical,
        "ts_event": event["event_time"],
        "ts_recv": received_time(source, event),
        "aggressor_side": side_name(source),
        "price": price,
        "price_nano": price_nano(source, price),
        "size": volume,
        "passive_order_id": integer(source, "passive_exchange_order_id", "passiveExchangeOrderId", "passive_order_id", "passiveOrderId"),
        "aggressor_order_id": integer(source, "aggressor_exchange_order_id", "aggressorExchangeOrderId", "aggressor_order_id", "aggressorOrderId"),
    }


def main() -> None:
    load_dotenv()
    args = parse_args()
    consumer = KafkaConsumer(
        *args.topics,
        bootstrap_servers=args.bootstrap_servers,
        group_id=args.group_id,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda payload: json.loads(payload.decode("utf-8")),
    )
    client = clickhouse_client() if args.clickhouse else None
    csv_file = None
    writer = None
    if args.csv:
        args.csv.parent.mkdir(parents=True, exist_ok=True)
        exists = args.csv.exists() and args.csv.stat().st_size > 0
        csv_file = args.csv.open("a", newline="", encoding="utf-8")
        writer = csv.DictWriter(csv_file, fieldnames=[
            "event_id", "event_time", "received_at", "topic", "event_type", "symbol",
            "sequence", "price", "quantity", "side", "payload",
        ])
        if not exists:
            writer.writeheader()

    running = True

    def stop(_signum: int, _frame: Any) -> None:
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    count = 0
    try:
        while running:
            batches = consumer.poll(timeout_ms=1_000, max_records=500)
            for records in batches.values():
                for record in records:
                    source = record.value
                    event = normalize(record.topic, source)
                    if writer:
                        row = {**event, "event_time": event["event_time"].isoformat(),
                               "received_at": event["received_at"].isoformat()}
                        writer.writerow(row)
                    if client:
                        write_clickhouse(client, event)
                    count += 1
                    if args.max_messages and count >= args.max_messages:
                        running = False
                        break
                if not running:
                    break
            # 只有当前 poll 的记录已成功写入 CSV/ClickHouse 才提交
            # offset；处理异常会让消息在下次启动时重新投递。
            if batches:
                consumer.commit()
            if csv_file:
                csv_file.flush()
    finally:
        consumer.close()
        if csv_file:
            csv_file.close()
    print(f"processed={count}")


if __name__ == "__main__":
    main()
