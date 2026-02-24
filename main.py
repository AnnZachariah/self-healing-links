import asyncio
from typing import List, Tuple, Union

import typer
from rich.console import Console
from rich.live import Live
from rich.table import Table

from crawler import LinkCrawler
from database import DeadLinkDatabase
from classifier import SelfHealingClassifier
from replacement_engine import ReplacementEngine
from apply_engine import ApplyEngine

app = typer.Typer(help="Self-healing link verification crawler.", no_args_is_help=True)
console = Console()


@app.callback()
def root() -> None:
    """CLI for self-healing link verification."""


def build_progress_table(url: str, status_code: Union[int, str], dead_count: int) -> Table:
    table = Table(title="Link Check Progress")
    table.add_column("URL being checked", style="cyan", overflow="fold")
    table.add_column("Status code", style="magenta")
    table.add_column("Dead links found", style="red")
    table.add_row(url, str(status_code), str(dead_count))
    return table


def build_replacement_progress_table(dead_url: str, suggestion_count: int) -> Table:
    table = Table(title="Replacement Engine Progress")
    table.add_column("Dead URL being processed", style="cyan", overflow="fold")
    table.add_column("Suggestions found", style="green")
    table.add_row(dead_url, str(suggestion_count))
    return table


def build_classification_progress_table(suggested_url: str, classified_count: int, auto_count: int) -> Table:
    table = Table(title="Classifier Progress")
    table.add_column("Suggested URL", style="cyan", overflow="fold")
    table.add_column("Classified rows", style="magenta")
    table.add_column("Auto-replace", style="green")
    table.add_row(suggested_url, str(classified_count), str(auto_count))
    return table


@app.command("crawl")
def crawl(
    target: str = typer.Argument(..., help="Website URL or sitemap.xml URL"),
    db_path: str = typer.Option("dead_links.db", help="SQLite database path"),
    output_csv: str = typer.Option("dead_links.csv", help="Output CSV file path"),
) -> None:
    """Run Stage 1 crawler + dead link detector."""

    database = DeadLinkDatabase(db_path=db_path)
    run_id = database.create_run(target)
    crawler = LinkCrawler(database=database, max_depth=2, concurrency=10, run_id=run_id)

    async def runner() -> int:
        current_table = build_progress_table("Starting...", "-", 0)

        async def progress_callback(url: str, status_code: Union[int, str], dead_count: int) -> None:
            nonlocal current_table
            current_table = build_progress_table(url, status_code, dead_count)

        with Live(current_table, console=console, refresh_per_second=12) as live:
            async def live_progress_callback(url: str, status_code: Union[int, str], dead_count: int) -> None:
                await progress_callback(url, status_code, dead_count)
                live.update(current_table)

            return await crawler.crawl(target, progress_callback=live_progress_callback)

    try:
        dead_count = asyncio.run(runner())
    except ValueError as exc:
        console.print(f"[red]Error:[/red] {exc}")
        raise typer.Exit(code=1) from exc
    except KeyboardInterrupt:
        console.print("\n[yellow]Crawl interrupted by user.[/yellow]")
        raise typer.Exit(code=130)

    database.export_to_csv(output_csv)
    console.print(f"\n[green]Done.[/green] Dead links found: {dead_count}")
    console.print(f"Run ID: {run_id}")
    console.print(f"Database: {db_path}")
    console.print(f"CSV export: {output_csv}")


@app.command("replace")
def replace(
    db_path: str = typer.Option("dead_links.db", help="SQLite database path"),
    output_csv: str = typer.Option(
        "replacement_suggestions.csv",
        help="Output CSV file path for replacement suggestions",
    ),
    limit: int = typer.Option(200, help="Maximum number of dead links to process"),
    top_k: int = typer.Option(3, min=1, help="Top candidate suggestions to keep per dead link"),
    min_similarity: float = typer.Option(
        0.03,
        min=0.0,
        max=1.0,
        help="Minimum semantic similarity threshold (0.0-1.0)",
    ),
    run_id: int = typer.Option(0, help="Run ID to process (0 means latest run)"),
) -> None:
    """Run Stage 2 replacement engine using Wayback + semantic matching."""

    database = DeadLinkDatabase(db_path=db_path)
    resolved_run_id = run_id if run_id > 0 else (database.latest_run_id() or 0)
    dead_links = database.get_dead_links(
        limit=limit,
        run_id=resolved_run_id if resolved_run_id > 0 else None,
    )
    if not dead_links:
        console.print("[yellow]No dead links found in database. Run crawl first.[/yellow]")
        raise typer.Exit(code=0)

    engine = ReplacementEngine(
        database=database,
        min_similarity=min_similarity,
        top_k_per_link=top_k,
    )

    async def runner() -> int:
        current_table = build_replacement_progress_table("Starting...", 0)

        async def progress_callback(dead_url: str, suggestion_count: int) -> None:
            nonlocal current_table
            current_table = build_replacement_progress_table(dead_url, suggestion_count)

        with Live(current_table, console=console, refresh_per_second=8) as live:
            async def live_progress_callback(dead_url: str, suggestion_count: int) -> None:
                await progress_callback(dead_url, suggestion_count)
                live.update(current_table)

            return await engine.generate_replacements(
                dead_links=dead_links,
                progress_callback=live_progress_callback,
                run_id=resolved_run_id if resolved_run_id > 0 else None,
            )

    try:
        suggestion_count = asyncio.run(runner())
    except KeyboardInterrupt:
        console.print("\n[yellow]Replacement engine interrupted by user.[/yellow]")
        raise typer.Exit(code=130)

    database.export_replacements_to_csv(output_csv)
    console.print(f"\n[green]Done.[/green] Suggestions found: {suggestion_count}")
    console.print(f"Run ID: {resolved_run_id}")
    console.print(f"Database: {db_path}")
    console.print(f"CSV export: {output_csv}")


@app.command("classify")
def classify(
    db_path: str = typer.Option("dead_links.db", help="SQLite database path"),
    output_csv: str = typer.Option(
        "replacement_classifications.csv",
        help="Output CSV path for classification results",
    ),
    limit: int = typer.Option(500, help="Maximum replacement suggestions to classify"),
    min_similarity: float = typer.Option(
        0.0,
        min=0.0,
        max=1.0,
        help="Optional minimum similarity filter before classification",
    ),
    auto_threshold: float = typer.Option(
        0.75,
        min=0.0,
        max=1.0,
        help="Confidence threshold for auto replacement",
    ),
    run_id: int = typer.Option(0, help="Run ID to process (0 means latest run)"),
) -> None:
    """Run Stage 3 confidence classifier (auto vs manual replacement)."""

    database = DeadLinkDatabase(db_path=db_path)
    resolved_run_id = run_id if run_id > 0 else (database.latest_run_id() or 0)
    suggestions = database.get_replacement_suggestions(
        limit=limit,
        min_similarity=min_similarity,
        run_id=resolved_run_id if resolved_run_id > 0 else None,
    )
    if not suggestions:
        console.print("[yellow]No replacement suggestions found. Run replace first.[/yellow]")
        raise typer.Exit(code=0)

    classifier = SelfHealingClassifier(database=database, auto_threshold=auto_threshold)

    async def runner() -> tuple:
        current_table = build_classification_progress_table("Starting...", 0, 0)

        async def progress_callback(suggested_url: str, classified_count: int, auto_count: int) -> None:
            nonlocal current_table
            current_table = build_classification_progress_table(
                suggested_url=suggested_url,
                classified_count=classified_count,
                auto_count=auto_count,
            )

        with Live(current_table, console=console, refresh_per_second=8) as live:
            async def live_progress_callback(suggested_url: str, classified_count: int, auto_count: int) -> None:
                await progress_callback(suggested_url, classified_count, auto_count)
                live.update(current_table)

            classified_count, auto_count = await classifier.classify(
                suggestions=suggestions,
                progress_callback=live_progress_callback,
                run_id=resolved_run_id if resolved_run_id > 0 else None,
            )
            return classified_count, auto_count

    try:
        classified_count, auto_count = asyncio.run(runner())
    except KeyboardInterrupt:
        console.print("\n[yellow]Classification interrupted by user.[/yellow]")
        raise typer.Exit(code=130)

    database.export_classifications_to_csv(output_csv)
    console.print(f"\n[green]Done.[/green] Suggestions classified: {classified_count}")
    console.print(f"Auto-replace candidates: {auto_count}")
    console.print(f"Run ID: {resolved_run_id}")
    console.print(f"Database: {db_path}")
    console.print(f"CSV export: {output_csv}")


@app.command("apply-approved")
def apply_approved(
    db_path: str = typer.Option("dead_links.db", help="SQLite database path"),
    run_id: int = typer.Option(0, help="Run ID to process (0 means latest run)"),
    dry_run: bool = typer.Option(
        True,
        "--dry-run/--live",
        help="Dry-run logs intended changes without production writes.",
    ),
    connector: str = typer.Option(
        "none",
        help="Connector: none|files (files updates local HTML/MD/TXT content)",
    ),
    files_root: str = typer.Option(
        ".",
        help="Root folder used by --connector files",
    ),
    limit: int = typer.Option(500, min=1, help="Maximum approved decisions to process"),
) -> None:
    """Apply approved reviewer decisions (step 1: execution logging)."""
    database = DeadLinkDatabase(db_path=db_path)
    resolved_run_id = run_id if run_id > 0 else (database.latest_run_id() or 0)
    if resolved_run_id <= 0:
        console.print("[yellow]No run found. Run crawl first.[/yellow]")
        raise typer.Exit(code=0)

    engine = ApplyEngine(database=database)
    summary = engine.apply_approved(
        run_id=resolved_run_id,
        dry_run=dry_run,
        connector=connector,
        limit=limit,
        files_root=files_root,
    )
    console.print("\n[green]Done.[/green] Approved replacement execution complete.")
    console.print(f"Run ID: {resolved_run_id}")
    console.print(f"Mode: {'dry-run' if dry_run else 'live (no connector apply yet)'}")
    console.print(f"Processed: {summary['processed']}")
    console.print(f"Dry-run logged: {summary['dry_run']}")
    console.print(f"Applied: {summary['applied']}")
    console.print(f"Skipped: {summary['skipped']}")
    console.print(f"Failed: {summary['failed']}")


@app.command("validate")
def validate(
    db_path: str = typer.Option("dead_links.db", help="SQLite database path"),
    run_id: int = typer.Option(0, help="Run ID to validate (0 means latest run)"),
) -> None:
    """Validate pipeline data integrity for a run (PASS/WARN/FAIL)."""
    database = DeadLinkDatabase(db_path=db_path)
    resolved_run_id = run_id if run_id > 0 else (database.latest_run_id() or 0)
    checks: List[Tuple[str, str, str]] = []

    def add_check(name: str, status: str, detail: str) -> None:
        checks.append((name, status, detail))

    if resolved_run_id <= 0:
        add_check("Run exists", "FAIL", "No run found in crawl_runs.")
    else:
        row = database.db.conn.execute(
            "SELECT target, started_at FROM crawl_runs WHERE id = ? LIMIT 1",
            (resolved_run_id,),
        ).fetchone()
        if row is None:
            add_check("Run exists", "FAIL", f"Run ID {resolved_run_id} not found in crawl_runs.")
        else:
            add_check("Run exists", "PASS", f"run_id={resolved_run_id}, target={row[0]}")

    if resolved_run_id > 0:
        dead_count = database.db.conn.execute(
            "SELECT COUNT(*) FROM dead_links WHERE run_id = ?",
            (resolved_run_id,),
        ).fetchone()[0]
        add_check(
            "Dead links",
            "PASS" if dead_count > 0 else "WARN",
            f"Rows in dead_links for run {resolved_run_id}: {dead_count}",
        )

        suggestion_count = database.db.conn.execute(
            "SELECT COUNT(*) FROM replacement_suggestions WHERE run_id = ?",
            (resolved_run_id,),
        ).fetchone()[0]
        add_check(
            "Replacement suggestions",
            "PASS" if suggestion_count > 0 else "WARN",
            f"Rows in replacement_suggestions: {suggestion_count}",
        )

        classification_count = database.db.conn.execute(
            "SELECT COUNT(*) FROM replacement_classifications WHERE run_id = ?",
            (resolved_run_id,),
        ).fetchone()[0]
        add_check(
            "Classifications",
            "PASS" if classification_count > 0 else "WARN",
            f"Rows in replacement_classifications: {classification_count}",
        )

        invalid_classification_links = database.db.conn.execute(
            """
            SELECT COUNT(*)
            FROM replacement_classifications rc
            LEFT JOIN replacement_suggestions rs ON rs.id = rc.suggestion_id
            WHERE rc.run_id = ?
              AND (rs.id IS NULL OR rs.run_id != rc.run_id)
            """,
            (resolved_run_id,),
        ).fetchone()[0]
        add_check(
            "Classification linkage",
            "PASS" if invalid_classification_links == 0 else "FAIL",
            f"Classifications with missing/mismatched suggestion rows: {invalid_classification_links}",
        )

        invalid_decisions = database.db.conn.execute(
            """
            SELECT COUNT(*)
            FROM replacement_classifications
            WHERE run_id = ?
              AND decision NOT IN ('auto_replace', 'manual_review')
            """,
            (resolved_run_id,),
        ).fetchone()[0]
        add_check(
            "Classification decision values",
            "PASS" if invalid_decisions == 0 else "FAIL",
            f"Invalid decision rows: {invalid_decisions}",
        )

        invalid_reviewer_links = database.db.conn.execute(
            """
            SELECT COUNT(*)
            FROM reviewer_decisions rd
            LEFT JOIN replacement_suggestions rs ON rs.id = rd.suggestion_id
            WHERE rd.run_id = ?
              AND (rs.id IS NULL OR rs.run_id != rd.run_id)
            """,
            (resolved_run_id,),
        ).fetchone()[0]
        add_check(
            "Reviewer decision linkage",
            "PASS" if invalid_reviewer_links == 0 else "FAIL",
            f"Reviewer decisions with invalid suggestion_id/run_id: {invalid_reviewer_links}",
        )

        apply_logs = database.db.conn.execute(
            "SELECT COUNT(*) FROM applied_replacements WHERE run_id = ?",
            (resolved_run_id,),
        ).fetchone()[0]
        add_check(
            "Applied replacement logs",
            "PASS" if apply_logs > 0 else "WARN",
            f"Rows in applied_replacements: {apply_logs}",
        )

    table = Table(title="Validation Report")
    table.add_column("Check", style="cyan", overflow="fold")
    table.add_column("Status", style="magenta")
    table.add_column("Details", style="white", overflow="fold")
    for name, status, detail in checks:
        color = {"PASS": "green", "WARN": "yellow", "FAIL": "red"}.get(status, "white")
        table.add_row(name, f"[{color}]{status}[/{color}]", detail)
    console.print(table)

    fail_count = sum(1 for _, status, _ in checks if status == "FAIL")
    warn_count = sum(1 for _, status, _ in checks if status == "WARN")
    console.print(f"Summary: FAIL={fail_count}, WARN={warn_count}, CHECKS={len(checks)}")
    if fail_count > 0:
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()
