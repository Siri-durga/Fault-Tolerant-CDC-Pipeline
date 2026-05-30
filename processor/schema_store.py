"""
Schema Store - tracks schema evolution per table using SQLite.
"""

import json
import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Optional

logger = logging.getLogger("cdc-processor.schema-store")


class SchemaStore:
    """
    Persists schema versions in a SQLite database.
    Each unique column set for a table gets a new version number.
    """

    CREATE_SQL = """
    CREATE TABLE IF NOT EXISTS schema_versions (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        table_name      TEXT NOT NULL,
        schema_version  INTEGER NOT NULL,
        schema_hash     TEXT NOT NULL,
        schema_json     TEXT NOT NULL,
        active_from     TEXT NOT NULL,
        UNIQUE(table_name, schema_version)
    );
    CREATE INDEX IF NOT EXISTS idx_table_version ON schema_versions(table_name, schema_version);
    """

    def __init__(self, db_path: Path):
        self.db_path = db_path
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(db_path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._init_db()
        # In-memory cache: table_name -> (schema_hash -> version)
        self._cache: Dict[str, Dict[str, int]] = {}
        self._load_cache()
        logger.info("SchemaStore initialised at %s", db_path)

    def _init_db(self):
        for stmt in self.CREATE_SQL.strip().split(";"):
            stmt = stmt.strip()
            if stmt:
                self._conn.execute(stmt)
        self._conn.commit()

    def _load_cache(self):
        rows = self._conn.execute(
            "SELECT table_name, schema_hash, schema_version FROM schema_versions"
        ).fetchall()
        for row in rows:
            t = row["table_name"]
            if t not in self._cache:
                self._cache[t] = {}
            self._cache[t][row["schema_hash"]] = row["schema_version"]

    @staticmethod
    def _hash_schema(schema: Dict[str, str]) -> str:
        """Stable hash of a schema dict based on sorted column names."""
        canonical = json.dumps(sorted(schema.items()), sort_keys=True)
        import hashlib
        return hashlib.sha256(canonical.encode()).hexdigest()[:16]

    def register_schema(self, table_name: str, schema: Dict[str, str]) -> int:
        """
        Register a schema for a table. Returns the version number.
        If the schema already exists, returns its existing version.
        If it's new, assigns the next version number.
        """
        schema_hash = self._hash_schema(schema)
        table_cache = self._cache.setdefault(table_name, {})

        if schema_hash in table_cache:
            return table_cache[schema_hash]

        # New schema version
        cur = self._conn.execute(
            "SELECT COALESCE(MAX(schema_version), 0) FROM schema_versions WHERE table_name = ?",
            (table_name,)
        )
        max_ver = cur.fetchone()[0]
        new_ver = max_ver + 1
        now_iso = datetime.now(timezone.utc).isoformat()

        self._conn.execute(
            """INSERT INTO schema_versions
               (table_name, schema_version, schema_hash, schema_json, active_from)
               VALUES (?, ?, ?, ?, ?)""",
            (table_name, new_ver, schema_hash, json.dumps(schema), now_iso)
        )
        self._conn.commit()

        table_cache[schema_hash] = new_ver
        logger.info("New schema v%d for table '%s': %s", new_ver, table_name, list(schema.keys()))
        return new_ver

    def get_versions(self, table_name: str):
        """Return all schema versions for a table."""
        return self._conn.execute(
            "SELECT * FROM schema_versions WHERE table_name = ? ORDER BY schema_version",
            (table_name,)
        ).fetchall()

    def get_latest_version(self, table_name: str) -> Optional[int]:
        cur = self._conn.execute(
            "SELECT MAX(schema_version) FROM schema_versions WHERE table_name = ?",
            (table_name,)
        )
        row = cur.fetchone()
        return row[0] if row else None

    def get_all(self):
        return self._conn.execute(
            "SELECT * FROM schema_versions ORDER BY table_name, schema_version"
        ).fetchall()

    def close(self):
        self._conn.close()
