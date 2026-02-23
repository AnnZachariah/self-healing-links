import asyncio
from typing import Union

import typer
from rich.console import Console
from rich.live import Live
from rich.table import Table

from crawler import LinkCrawler
from database import DeadLinkDatabase

app = typer.Typer(help="Self-healing link verification crawler.")
console = Console()


def build_progress_table(url: str, status_code: Union[int, str], dead_count: int) -> Table:
    table = Table(title="Link Check Progress")
    table.add_column("URL being checked", style="cyan", overflow="fold")
    table.add_column("Status code", style="magenta")
    table.add_column("Dead links found", style="red")
    table.add_row(url, str(status_code), str(dead_count))
    return table


@app.command()
def run(
    target: str = typer.Argument(..., help="Website URL or sitemap.xml URL"),
    db_path: str = typer.Option("dead_links.db", help="SQLite database path"),
    output_csv: str = typer.Option("dead_links.csv", help="Output CSV file path"),
) -> None:
    """Run Stage 1 crawler + dead link detector."""

    database = DeadLinkDatabase(db_path=db_path)
    crawler = LinkCrawler(database=database, max_depth=2, concurrency=10)

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
    console.print(f"Database: {db_path}")
    console.print(f"CSV export: {output_csv}")


if __name__ == "__main__":
    app()
