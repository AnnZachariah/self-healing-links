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
        self.db["crawl_runs"].create(
            {
                "id": int,
                "target": str,
                "started_at": str,
            },
            pk="id",
            if_not_exists=True,
        )

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
        self.db["replacement_classifications"].create(
            {
                "id": int,
                "suggestion_id": int,
                "dead_url": str,
                "suggested_url": str,
                "similarity_score": float,
                "confidence_score": float,
                "decision": str,
                "rationale": str,
                "classified_at": str,
            },
            pk="id",
            if_not_exists=True,
        )

        self._ensure_column("dead_links", "run_id", "INTEGER")
        self._ensure_column("replacement_suggestions", "run_id", "INTEGER")
        self._ensure_column("replacement_classifications", "run_id", "INTEGER")

    def _ensure_column(self, table_name: str, column_name: str, column_type: str) -> None:
        columns = [row[1] for row in self.db.conn.execute(f"PRAGMA table_info({table_name})").fetchall()]
        if column_name in columns:
            return
        self.db.conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}")
        self.db.conn.commit()

    def create_run(self, target: str) -> int:
        started_at = datetime.now(timezone.utc).isoformat()
        self.db["crawl_runs"].insert({"target": target, "started_at": started_at})
        run_id = self.db.conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        return int(run_id)

    def latest_run_id(self) -> Optional[int]:
        row = self.db.conn.execute("SELECT id FROM crawl_runs ORDER BY id DESC LIMIT 1").fetchone()
        if row is None:
            return None
        return int(row[0])

    def latest_run_id_with_dead_links(self) -> Optional[int]:
        row = self.db.conn.execute(
            """
            SELECT run_id
            FROM dead_links
            WHERE run_id IS NOT NULL
            GROUP BY run_id
            ORDER BY run_id DESC
            LIMIT 1
            """
        ).fetchone()
        if row is None:
            return None
        return int(row[0])

    def latest_run_id_with_suggestions(self) -> Optional[int]:
        row = self.db.conn.execute(
            """
            SELECT run_id
            FROM replacement_suggestions
            WHERE run_id IS NOT NULL
            GROUP BY run_id
            ORDER BY run_id DESC
            LIMIT 1
            """
        ).fetchone()
        if row is None:
            return None
        return int(row[0])

    def insert_dead_link(
        self,
        source_page: str,
        dead_url: str,
        anchor_text: str,
        surrounding_context: str,
        status_code: str,
        discovered_at: str,
        run_id: Optional[int] = None,
    ) -> None:
        if self._dead_link_exists(source_page=source_page, dead_url=dead_url, run_id=run_id):
            return

        self.table.insert(
            {
                "run_id": run_id,
                "source_page": source_page,
                "dead_url": dead_url,
                "anchor_text": anchor_text,
                "surrounding_context": surrounding_context,
                "status_code": status_code,
                "discovered_at": discovered_at,
            }
        )

    def _dead_link_exists(self, source_page: str, dead_url: str, run_id: Optional[int]) -> bool:
        if run_id is None:
            row = self.db.conn.execute(
                "SELECT 1 FROM dead_links WHERE source_page = ? AND dead_url = ? AND run_id IS NULL LIMIT 1",
                (source_page, dead_url),
            ).fetchone()
            return row is not None

        row = self.db.conn.execute(
            "SELECT 1 FROM dead_links WHERE source_page = ? AND dead_url = ? AND run_id = ? LIMIT 1",
            (source_page, dead_url, run_id),
        ).fetchone()
        return row is not None

    def export_to_csv(self, csv_path: str = "dead_links.csv") -> None:
        rows = list(self.table.rows)
        fieldnames = [
            "id",
            "run_id",
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

    def get_dead_links(
        self,
        limit: Optional[int] = None,
        run_id: Optional[int] = None,
    ) -> List[Dict[str, str]]:
        query = "SELECT * FROM dead_links"
        params: List[object] = []

        if run_id is not None:
            query += " WHERE run_id = ?"
            params.append(run_id)

        query += " ORDER BY id"
        if limit is not None:
            query += " LIMIT ?"
            params.append(limit)

        rows = self.db.conn.execute(query, tuple(params)).fetchall()
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
        run_id: Optional[int] = None,
    ) -> None:
        if self._replacement_exists(dead_url=dead_url, suggested_url=suggested_url, run_id=run_id):
            return

        self.db["replacement_suggestions"].insert(
            {
                "run_id": run_id,
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

    def _replacement_exists(self, dead_url: str, suggested_url: str, run_id: Optional[int]) -> bool:
        if run_id is None:
            row = self.db.conn.execute(
                "SELECT 1 FROM replacement_suggestions WHERE dead_url = ? AND suggested_url = ? AND run_id IS NULL LIMIT 1",
                (dead_url, suggested_url),
            ).fetchone()
            return row is not None
        row = self.db.conn.execute(
            "SELECT 1 FROM replacement_suggestions WHERE dead_url = ? AND suggested_url = ? AND run_id = ? LIMIT 1",
            (dead_url, suggested_url, run_id),
        ).fetchone()
        return row is not None

    def export_replacements_to_csv(self, csv_path: str = "replacement_suggestions.csv") -> None:
        rows = list(self.db["replacement_suggestions"].rows)
        fieldnames = [
            "id",
            "run_id",
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

    def get_replacement_suggestions(
        self,
        limit: Optional[int] = None,
        min_similarity: Optional[float] = None,
        run_id: Optional[int] = None,
    ) -> List[Dict[str, object]]:
        query = "SELECT * FROM replacement_suggestions"
        params: List[object] = []
        where_clauses: List[str] = []

        if min_similarity is not None:
            where_clauses.append("similarity_score >= ?")
            params.append(min_similarity)
        if run_id is not None:
            where_clauses.append("run_id = ?")
            params.append(run_id)

        if where_clauses:
            query += " WHERE " + " AND ".join(where_clauses)

        query += " ORDER BY similarity_score DESC, id ASC"
        if limit is not None:
            query += " LIMIT ?"
            params.append(limit)

        rows = self.db.conn.execute(query, tuple(params)).fetchall()
        columns = [col[1] for col in self.db.conn.execute("PRAGMA table_info(replacement_suggestions)").fetchall()]
        return [dict(zip(columns, row)) for row in rows]

    def insert_classification(
        self,
        suggestion_id: int,
        dead_url: str,
        suggested_url: str,
        similarity_score: float,
        confidence_score: float,
        decision: str,
        rationale: str,
        run_id: Optional[int] = None,
    ) -> None:
        if self._classification_exists(suggestion_id=suggestion_id, run_id=run_id):
            self.db.conn.execute(
                """
                UPDATE replacement_classifications
                SET dead_url = ?, suggested_url = ?, similarity_score = ?, confidence_score = ?, decision = ?, rationale = ?, classified_at = ?, run_id = ?
                WHERE suggestion_id = ? AND ((run_id = ?) OR (run_id IS NULL AND ? IS NULL))
                """,
                (
                    dead_url,
                    suggested_url,
                    round(similarity_score, 4),
                    round(confidence_score, 4),
                    decision,
                    rationale,
                    datetime.now(timezone.utc).isoformat(),
                    run_id,
                    suggestion_id,
                    run_id,
                    run_id,
                ),
            )
            self.db.conn.commit()
            return

        self.db["replacement_classifications"].insert(
            {
                "run_id": run_id,
                "suggestion_id": suggestion_id,
                "dead_url": dead_url,
                "suggested_url": suggested_url,
                "similarity_score": round(similarity_score, 4),
                "confidence_score": round(confidence_score, 4),
                "decision": decision,
                "rationale": rationale,
                "classified_at": datetime.now(timezone.utc).isoformat(),
            }
        )

    def _classification_exists(self, suggestion_id: int, run_id: Optional[int]) -> bool:
        if run_id is None:
            row = self.db.conn.execute(
                "SELECT 1 FROM replacement_classifications WHERE suggestion_id = ? AND run_id IS NULL LIMIT 1",
                (suggestion_id,),
            ).fetchone()
            return row is not None
        row = self.db.conn.execute(
            "SELECT 1 FROM replacement_classifications WHERE suggestion_id = ? AND run_id = ? LIMIT 1",
            (suggestion_id, run_id),
        ).fetchone()
        return row is not None

    def export_classifications_to_csv(self, csv_path: str = "replacement_classifications.csv") -> None:
        rows = list(self.db["replacement_classifications"].rows)
        fieldnames = [
            "id",
            "run_id",
            "suggestion_id",
            "dead_url",
            "suggested_url",
            "similarity_score",
            "confidence_score",
            "decision",
            "rationale",
            "classified_at",
        ]

        with Path(csv_path).open("w", newline="", encoding="utf-8") as output:
            writer = csv.DictWriter(output, fieldnames=fieldnames)
            writer.writeheader()
            if rows:
                writer.writerows(rows)

    def get_classifications(
        self,
        limit: Optional[int] = None,
        run_id: Optional[int] = None,
    ) -> List[Dict[str, object]]:
        query = "SELECT * FROM replacement_classifications"
        params: List[object] = []
        if run_id is not None:
            query += " WHERE run_id = ?"
            params.append(run_id)
        query += " ORDER BY confidence_score DESC, id ASC"
        if limit is not None:
            query += " LIMIT ?"
            params.append(limit)
        rows = self.db.conn.execute(query, tuple(params)).fetchall()
        columns = [col[1] for col in self.db.conn.execute("PRAGMA table_info(replacement_classifications)").fetchall()]
        return [dict(zip(columns, row)) for row in rows]

    def get_auto_replacements(
        self,
        limit: Optional[int] = None,
        min_confidence: float = 0.75,
        run_id: Optional[int] = None,
    ) -> List[Dict[str, object]]:
        query = """
        SELECT
            rs.id AS suggestion_id,
            rs.source_page,
            rc.dead_url,
            rc.suggested_url,
            rc.similarity_score,
            rc.confidence_score,
            rc.decision
        FROM replacement_classifications rc
        JOIN replacement_suggestions rs ON rs.id = rc.suggestion_id
        WHERE rc.decision = 'auto_replace'
          AND rc.confidence_score >= ?
        """
        params: List[object] = [min_confidence]
        if run_id is not None:
            query += " AND rc.run_id = ?"
            params.append(run_id)
        query += " ORDER BY rc.confidence_score DESC, rc.id ASC"
        if limit is not None:
            query += " LIMIT ?"
            params.append(limit)

        cursor = self.db.conn.execute(query, tuple(params))
        columns = [col[0] for col in cursor.description]
        rows = cursor.fetchall()
        return [dict(zip(columns, row)) for row in rows]
