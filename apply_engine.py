from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse

from database import DeadLinkDatabase


class FilesConnector:
    def __init__(self, root_dir: str, backup_dir: str = ".apply_backups") -> None:
        self.root_dir = Path(root_dir).resolve()
        self.backup_dir = self.root_dir / backup_dir
        self.allowed_suffixes = {".html", ".htm", ".md", ".txt"}

    def _candidate_paths_from_source(self, source_page: str) -> List[Path]:
        parsed = urlparse(source_page)
        path = (parsed.path or "/").strip()
        cleaned = path.strip("/")
        candidates: List[Path] = []

        if not cleaned:
            candidates.extend(
                [
                    self.root_dir / "index.html",
                    self.root_dir / "index.htm",
                    self.root_dir / "README.md",
                ]
            )
            return candidates

        base = self.root_dir / cleaned
        if base.suffix:
            candidates.append(base)
        else:
            candidates.extend(
                [
                    base / "index.html",
                    base / "index.htm",
                    base.with_suffix(".html"),
                    base.with_suffix(".htm"),
                    base.with_suffix(".md"),
                ]
            )
        return candidates

    def _search_fallback(self, old_url: str, max_files: int = 5000) -> Optional[Path]:
        checked = 0
        for file_path in self.root_dir.rglob("*"):
            if not file_path.is_file():
                continue
            if file_path.suffix.lower() not in self.allowed_suffixes:
                continue
            if self.backup_dir in file_path.parents:
                continue
            checked += 1
            if checked > max_files:
                break
            try:
                content = file_path.read_text(encoding="utf-8")
            except UnicodeDecodeError:
                continue
            except OSError:
                continue
            if old_url in content:
                return file_path
        return None

    def locate_file(self, source_page: str, old_url: str) -> Optional[Path]:
        for candidate in self._candidate_paths_from_source(source_page):
            if not candidate.exists() or not candidate.is_file():
                continue
            if candidate.suffix.lower() not in self.allowed_suffixes:
                continue
            try:
                content = candidate.read_text(encoding="utf-8")
            except UnicodeDecodeError:
                continue
            except OSError:
                continue
            if old_url in content:
                return candidate
        return self._search_fallback(old_url=old_url)

    def apply(self, source_page: str, old_url: str, new_url: str, dry_run: bool) -> Tuple[str, str, str]:
        if not self.root_dir.exists():
            return ("failed", f"Root directory does not exist: {self.root_dir}", "")

        target_file = self.locate_file(source_page=source_page, old_url=old_url)
        if target_file is None:
            return ("failed", "No file found containing the old URL.", "")

        try:
            content = target_file.read_text(encoding="utf-8")
        except OSError as exc:
            return ("failed", f"Failed reading file: {exc}", str(target_file))

        if old_url not in content:
            return ("failed", "File resolved but old URL was not present.", str(target_file))

        replacements = content.count(old_url)
        if dry_run:
            return ("dry_run", f"Would replace {replacements} occurrence(s).", str(target_file))

        try:
            self.backup_dir.mkdir(parents=True, exist_ok=True)
            backup_file = self.backup_dir / f"{target_file.name}.bak"
            backup_file.write_text(content, encoding="utf-8")
            updated = content.replace(old_url, new_url)
            target_file.write_text(updated, encoding="utf-8")
        except OSError as exc:
            return ("failed", f"Failed writing file: {exc}", str(target_file))

        return ("applied", f"Replaced {replacements} occurrence(s).", str(target_file))


class ApplyEngine:
    def __init__(self, database: DeadLinkDatabase) -> None:
        self.database = database

    def apply_approved(
        self,
        run_id: int,
        dry_run: bool = True,
        connector: str = "none",
        limit: Optional[int] = None,
        files_root: str = ".",
    ) -> Dict[str, int]:
        decisions = self.database.get_approved_reviewer_decisions(run_id=run_id, limit=limit)
        summary = {
            "processed": 0,
            "dry_run": 0,
            "applied": 0,
            "skipped": 0,
            "failed": 0,
        }

        files_connector = FilesConnector(root_dir=files_root) if connector == "files" else None

        for decision in decisions:
            suggestion_id = int(decision.get("suggestion_id", 0) or 0)
            suggestion = self.database.get_replacement_suggestion_by_id(suggestion_id=suggestion_id, run_id=run_id)
            summary["processed"] += 1

            if suggestion is None:
                summary["failed"] += 1
                self.database.upsert_applied_replacement(
                    run_id=run_id,
                    suggestion_id=suggestion_id,
                    source_page="",
                    dead_url=str(decision.get("dead_url", "")),
                    old_url=str(decision.get("dead_url", "")),
                    new_url=str(decision.get("final_suggested_url", "")),
                    status="failed",
                    connector=connector,
                    dry_run=dry_run,
                    message="Suggestion not found for approved decision.",
                )
                continue

            source_page = str(suggestion.get("source_page", ""))
            dead_url = str(suggestion.get("dead_url", ""))
            new_url = str(decision.get("final_suggested_url", "") or suggestion.get("suggested_url", ""))

            if connector != "files":
                status = "dry_run" if dry_run else "skipped_no_connector"
                message = (
                    "Validated approved replacement. No write operation performed."
                    if dry_run
                    else "No production connector configured. Set connector='files' or implement CMS connector."
                )
                if status == "dry_run":
                    summary["dry_run"] += 1
                else:
                    summary["skipped"] += 1
                self.database.upsert_applied_replacement(
                    run_id=run_id,
                    suggestion_id=suggestion_id,
                    source_page=source_page,
                    dead_url=dead_url,
                    old_url=dead_url,
                    new_url=new_url,
                    status=status,
                    connector=connector,
                    dry_run=dry_run,
                    message=message,
                )
                continue

            status, message, file_path = files_connector.apply(
                source_page=source_page,
                old_url=dead_url,
                new_url=new_url,
                dry_run=dry_run,
            )
            if status == "dry_run":
                summary["dry_run"] += 1
            elif status == "applied":
                summary["applied"] += 1
            elif status.startswith("skipped"):
                summary["skipped"] += 1
            else:
                summary["failed"] += 1

            full_message = message if not file_path else f"{message} File: {file_path}"
            self.database.upsert_applied_replacement(
                run_id=run_id,
                suggestion_id=suggestion_id,
                source_page=source_page,
                dead_url=dead_url,
                old_url=dead_url,
                new_url=new_url,
                status=status,
                connector=connector,
                dry_run=dry_run,
                message=full_message,
            )

        return summary
