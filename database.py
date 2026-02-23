import csv
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

from sqlite_utils import Database


class DeadLinkDatabase:
    def __init__(self, db_path: str = "dead_links.db") -> None:
        self.db_path = Path(db_path)
        self.db = Database(self.db_path)
        self.table = self.db["dead_links"]
        self._ensure_schema()

    def _ensure_schema(self) -> None:
        self.table.create(
            {
                "id": int,
                "source_page": str,
                "dead_url": str,
                "anchor_text": str,
                "surrounding_context": str,
                "status_code": str,
                "discovered_at": str,
            },
            pk="id",
            if_not_exists=True,
        )
        self.db["replacement_suggestions"].create(
            {
                "id": int,
                "source_page": str,
                "dead_url": str,
                "anchor_text": str,
                "suggested_url": str,
                "wayback_snapshot_url": str,
                "similarity_score": float,
                "match_reason": str,
                "generated_at": str,
            },
            pk="id",
            if_not_exists=True,
        )

    def insert_dead_link(
        self,
        source_page: str,
        dead_url: str,
        anchor_text: str,
        surrounding_context: str,
        status_code: str,
        discovered_at: str,
    ) -> None:
        if self._dead_link_exists(source_page=source_page, dead_url=dead_url):
            return

        self.table.insert(
            {
                "source_page": source_page,
                "dead_url": dead_url,
                "anchor_text": anchor_text,
                "surrounding_context": surrounding_context,
                "status_code": status_code,
                "discovered_at": discovered_at,
            }
        )

    def _dead_link_exists(self, source_page: str, dead_url: str) -> bool:
        row = self.db.conn.execute(
            "SELECT 1 FROM dead_links WHERE source_page = ? AND dead_url = ? LIMIT 1",
            (source_page, dead_url),
        ).fetchone()
        return row is not None

    def export_to_csv(self, csv_path: str = "dead_links.csv") -> None:
        rows = list(self.table.rows)
        fieldnames = [
            "id",
            "source_page",
            "dead_url",
            "anchor_text",
            "surrounding_context",
            "status_code",
            "discovered_at",
        ]

        with Path(csv_path).open("w", newline="", encoding="utf-8") as output:
            writer = csv.DictWriter(output, fieldnames=fieldnames)
            writer.writeheader()
            if rows:
                writer.writerows(rows)

    def get_dead_links(self, limit: Optional[int] = None) -> List[Dict[str, str]]:
        query = "SELECT * FROM dead_links ORDER BY id"
        params: tuple = ()
        if limit is not None:
            query += " LIMIT ?"
            params = (limit,)
        rows = self.db.conn.execute(query, params).fetchall()
        columns = [col[1] for col in self.db.conn.execute("PRAGMA table_info(dead_links)").fetchall()]
        return [dict(zip(columns, row)) for row in rows]

    def insert_replacement_suggestion(
        self,
        source_page: str,
        dead_url: str,
        anchor_text: str,
        suggested_url: str,
        wayback_snapshot_url: str,
        similarity_score: float,
        match_reason: str,
    ) -> None:
        if self._replacement_exists(dead_url=dead_url, suggested_url=suggested_url):
            return

        self.db["replacement_suggestions"].insert(
            {
                "source_page": source_page,
                "dead_url": dead_url,
                "anchor_text": anchor_text,
                "suggested_url": suggested_url,
                "wayback_snapshot_url": wayback_snapshot_url,
                "similarity_score": round(similarity_score, 4),
                "match_reason": match_reason,
                "generated_at": datetime.now(timezone.utc).isoformat(),
            }
        )

    def _replacement_exists(self, dead_url: str, suggested_url: str) -> bool:
        row = self.db.conn.execute(
            "SELECT 1 FROM replacement_suggestions WHERE dead_url = ? AND suggested_url = ? LIMIT 1",
            (dead_url, suggested_url),
        ).fetchone()
        return row is not None

    def export_replacements_to_csv(self, csv_path: str = "replacement_suggestions.csv") -> None:
        rows = list(self.db["replacement_suggestions"].rows)
        fieldnames = [
            "id",
            "source_page",
            "dead_url",
            "anchor_text",
            "suggested_url",
            "wayback_snapshot_url",
            "similarity_score",
            "match_reason",
            "generated_at",
        ]

        with Path(csv_path).open("w", newline="", encoding="utf-8") as output:
            writer = csv.DictWriter(output, fieldnames=fieldnames)
            writer.writeheader()
            if rows:
                writer.writerows(rows)
