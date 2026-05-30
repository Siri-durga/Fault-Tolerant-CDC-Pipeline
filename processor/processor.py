#!/usr/bin/env python3
"""
CDC Pipeline Processor
Consumes Debezium change events from Kafka, handles schema evolution,
writes Parquet files to data lake, and tracks lineage.
"""

import os
import json
import logging
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from kafka import KafkaConsumer
from kafka.errors import KafkaError

from schema_store import SchemaStore
from lineage_tracker import LineageTracker

# ── Logging ──────────────────────────────────────────────────────────────────
LOG_LEVEL = os.getenv("PROCESSOR_LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("cdc-processor")

# ── Config ────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP   = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_PREFIX      = os.getenv("PROCESSOR_KAFKA_TOPIC_PREFIX", "ecommerce_server.ecommerce")
DATA_LAKE_PATH    = Path(os.getenv("PROCESSOR_DATA_LAKE_PATH", "/data_lake"))
STATE_PATH        = Path(os.getenv("PROCESSOR_STATE_PATH", "/state"))
LINEAGE_PATH      = Path(os.getenv("LINEAGE_REPORT_PATH", "/output/lineage_report.json"))

TABLES            = ["customers", "products", "orders"]
TOPICS            = [f"{TOPIC_PREFIX}.{t}" for t in TABLES]

OP_MAP = {"c": "c", "r": "c", "u": "u", "d": "d"}   # Debezium op → partition name


def ensure_dirs() -> None:
    DATA_LAKE_PATH.mkdir(parents=True, exist_ok=True)
    STATE_PATH.mkdir(parents=True, exist_ok=True)
    LINEAGE_PATH.parent.mkdir(parents=True, exist_ok=True)


# ── Parquet helpers ───────────────────────────────────────────────────────────

def infer_arrow_type(value: Any) -> pa.DataType:
    if isinstance(value, bool):
        return pa.bool_()
    if isinstance(value, int):
        return pa.int64()
    if isinstance(value, float):
        return pa.float64()
    return pa.string()


def record_to_arrow_schema(record: Dict) -> pa.Schema:
    fields = []
    for k, v in record.items():
        fields.append(pa.field(k, infer_arrow_type(v)))
    return pa.schema(fields)


def coerce_record(record: Dict, schema: pa.Schema) -> Dict:
    """Coerce record values to match the given Arrow schema."""
    out = {}
    for field in schema:
        val = record.get(field.name)
        if val is None:
            out[field.name] = None
        elif field.type == pa.bool_():
            out[field.name] = bool(val)
        elif field.type == pa.int64():
            out[field.name] = int(val) if val is not None else None
        elif field.type == pa.float64():
            out[field.name] = float(val) if val is not None else None
        else:
            out[field.name] = str(val) if val is not None else None
    # Add any new columns not in existing schema
    for k, v in record.items():
        if k not in out:
            out[k] = str(v) if v is not None else None
    return out


def write_parquet(record: Dict, table_name: str, event_date: str, op_type: str, schema_version: int) -> Path:
    """Write a single record to a partitioned Parquet file."""
    partition_dir = DATA_LAKE_PATH / table_name / event_date / op_type
    partition_dir.mkdir(parents=True, exist_ok=True)

    ts_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    parquet_file = partition_dir / f"{ts_ms}_v{schema_version}.parquet"

    # Clean internal Debezium meta fields from data before writing
    clean = {k: v for k, v in record.items() if not k.startswith("__")}
    if not clean:
        clean = {"_empty": "true"}

    df = pd.DataFrame([clean])
    # Convert all columns to string-safe types
    for col in df.columns:
        df[col] = df[col].astype(str).where(df[col].notna(), None)

    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, str(parquet_file))
    logger.debug("Wrote Parquet: %s", parquet_file)
    return parquet_file


# ── Message parsing ───────────────────────────────────────────────────────────

def parse_message(raw_value: bytes) -> Optional[Dict]:
    """Parse a Debezium Kafka message value."""
    try:
        msg = json.loads(raw_value.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        logger.warning("Could not parse message: %s", e)
        return None

    # Handle both wrapped and unwrapped (ExtractNewRecordState) formats
    if "payload" in msg:
        payload = msg["payload"]
    else:
        payload = msg

    if payload is None:
        return None

    return payload


def extract_table_from_topic(topic: str) -> str:
    return topic.split(".")[-1]


def extract_schema_fields(msg_value: bytes) -> Optional[Dict]:
    """Extract schema fields from Debezium envelope if present."""
    try:
        msg = json.loads(msg_value.decode("utf-8"))
        schema = msg.get("schema", {})
        if not schema:
            return None
        # Find the 'after' field schema
        for field in schema.get("fields", []):
            if field.get("field") == "after":
                fields = {}
                for f in field.get("fields", []):
                    fields[f["field"]] = f["type"]
                return fields
    except Exception:
        pass
    return None


# ── Main processor ────────────────────────────────────────────────────────────

class CDCProcessor:
    def __init__(self):
        ensure_dirs()
        self.schema_store = SchemaStore(STATE_PATH / "schemas.db")
        self.lineage = LineageTracker(LINEAGE_PATH)
        self.consumer: Optional[KafkaConsumer] = None
        self._running = True

    def _build_consumer(self) -> KafkaConsumer:
        retries = 0
        while retries < 30:
            try:
                consumer = KafkaConsumer(
                    *TOPICS,
                    bootstrap_servers=KAFKA_BOOTSTRAP,
                    group_id="cdc-processor-group",
                    auto_offset_reset="earliest",
                    enable_auto_commit=True,
                    value_deserializer=None,   # raw bytes
                    key_deserializer=None,
                    consumer_timeout_ms=5000,
                    max_poll_records=100,
                    session_timeout_ms=30000,
                    heartbeat_interval_ms=10000,
                )
                logger.info("Kafka consumer connected. Topics: %s", TOPICS)
                return consumer
            except KafkaError as e:
                retries += 1
                logger.warning("Kafka connect failed (%d/30): %s", retries, e)
                time.sleep(10)
        raise RuntimeError("Could not connect to Kafka after 30 retries")

    def process_message(self, topic: str, raw_value: bytes) -> None:
        table_name = extract_table_from_topic(topic)
        payload = parse_message(raw_value)
        if payload is None:
            return

        # Extract operation type
        op_raw = payload.get("__op") or payload.get("op", "c")
        op_type = OP_MAP.get(op_raw, "c")

        # Extract timestamp
        ts_ms = payload.get("__source_ts_ms") or payload.get("ts_ms") or int(time.time() * 1000)
        try:
            event_dt = datetime.fromtimestamp(int(ts_ms) / 1000, tz=timezone.utc)
        except (ValueError, TypeError, OSError):
            event_dt = datetime.now(timezone.utc)
        event_date = event_dt.strftime("%Y-%m-%d")

        # Build clean record (remove Debezium meta fields)
        record = {k: v for k, v in payload.items()
                  if k not in ("__op", "__table", "__source_ts_ms", "__deleted", "op", "ts_ms", "source", "transaction")}

        if not record:
            logger.debug("Empty record for table %s, skipping", table_name)
            return

        # Detect schema (column names + types)
        current_schema = {k: type(v).__name__ for k, v in record.items()}

        # Register / version schema
        schema_version = self.schema_store.register_schema(table_name, current_schema)
        logger.info("Table=%s op=%s schema_v=%d date=%s cols=%s",
                    table_name, op_type, schema_version, event_date, list(record.keys()))

        # Write parquet
        try:
            parquet_path = write_parquet(record, table_name, event_date, op_type, schema_version)
            partition_key = f"{table_name}/{event_date}/{op_type}"
            self.lineage.record_event(
                table_name=table_name,
                schema_version=schema_version,
                output_partition=partition_key,
                output_schema=current_schema,
            )
        except Exception as e:
            logger.error("Failed to write parquet for %s: %s", table_name, e)

    def run(self) -> None:
        logger.info("CDC Processor starting. Connecting to Kafka at %s ...", KAFKA_BOOTSTRAP)
        # Wait for Kafka Connect + connector to be ready
        time.sleep(20)

        self.consumer = self._build_consumer()

        logger.info("CDC Processor running. Listening for changes ...")
        while self._running:
            try:
                records = self.consumer.poll(timeout_ms=5000)
                for tp, messages in records.items():
                    for msg in messages:
                        if msg.value:
                            self.process_message(msg.topic, msg.value)
            except KeyboardInterrupt:
                break
            except Exception as e:
                logger.error("Poll error: %s", e)
                time.sleep(5)

        self.shutdown()

    def shutdown(self) -> None:
        logger.info("Shutting down CDC processor ...")
        self._running = False
        if self.consumer:
            self.consumer.close()
        self.lineage.flush()
        logger.info("Shutdown complete.")


def main():
    processor = CDCProcessor()

    def handle_signal(signum, frame):
        logger.info("Signal %s received, shutting down...", signum)
        processor.shutdown()
        sys.exit(0)

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    processor.run()


if __name__ == "__main__":
    main()
