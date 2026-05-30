"""
Lineage Tracker - tracks data lineage and generates lineage_report.json
"""

import json
import logging
import threading
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Set

logger = logging.getLogger("cdc-processor.lineage")


class LineageTracker:
    """
    Tracks which schema versions produced which output partitions,
    and serialises the lineage report to JSON.
    """

    def __init__(self, report_path: Path):
        self.report_path = report_path
        report_path.parent.mkdir(parents=True, exist_ok=True)

        # table -> version -> LineageEntry
        self._entries: Dict[str, Dict[int, dict]] = defaultdict(dict)
        self._lock = threading.Lock()

        # Load existing report if present
        self._load_existing()

    def _load_existing(self):
        if self.report_path.exists():
            try:
                with open(self.report_path) as f:
                    existing = json.load(f)
                for entry in existing:
                    t = entry.get("source_table")
                    v = entry.get("schema_version")
                    if t and v is not None:
                        self._entries[t][v] = entry
                logger.info("Loaded %d existing lineage entries.", sum(len(v) for v in self._entries.values()))
            except Exception as e:
                logger.warning("Could not load existing lineage report: %s", e)

    def record_event(
        self,
        table_name: str,
        schema_version: int,
        output_partition: str,
        output_schema: Dict[str, str],
    ) -> None:
        with self._lock:
            existing = self._entries[table_name].get(schema_version)
            if existing is None:
                self._entries[table_name][schema_version] = {
                    "source_table": table_name,
                    "schema_version": schema_version,
                    "active_from": datetime.now(timezone.utc).isoformat(),
                    "output_partitions": [output_partition],
                    "output_schema": output_schema,
                }
            else:
                partitions: List[str] = existing["output_partitions"]
                if output_partition not in partitions:
                    partitions.append(output_partition)
                # Merge any new columns into output_schema
                existing["output_schema"].update(output_schema)

        # Flush on every event (small overhead, ensures persistence)
        self.flush()

    def flush(self) -> None:
        """Write the lineage report to disk."""
        with self._lock:
            report = []
            for table_versions in self._entries.values():
                for entry in table_versions.values():
                    report.append(entry)
            report.sort(key=lambda e: (e["source_table"], e["schema_version"]))

        try:
            with open(self.report_path, "w") as f:
                json.dump(report, f, indent=2, default=str)
            logger.debug("Lineage report written: %d entries", len(report))
        except Exception as e:
            logger.error("Failed to write lineage report: %s", e)
