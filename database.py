import csv
from pathlib import Path

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
