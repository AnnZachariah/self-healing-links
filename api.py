from typing import Any, Dict

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from classifier import SelfHealingClassifier
from crawler import LinkCrawler
from database import DeadLinkDatabase
from replacement_engine import ReplacementEngine


class CrawlRequest(BaseModel):
    target: str
    db_path: str = "dead_links.db"
    output_csv: str = "dead_links.csv"


class ReplaceRequest(BaseModel):
    db_path: str = "dead_links.db"
    output_csv: str = "replacement_suggestions.csv"
    limit: int = Field(default=200, ge=1)
    top_k: int = Field(default=3, ge=1)
    min_similarity: float = Field(default=0.03, ge=0.0, le=1.0)
    run_id: int = 0


class ClassifyRequest(BaseModel):
    db_path: str = "dead_links.db"
    output_csv: str = "replacement_classifications.csv"
    limit: int = Field(default=500, ge=1)
    min_similarity: float = Field(default=0.0, ge=0.0, le=1.0)
    auto_threshold: float = Field(default=0.75, ge=0.0, le=1.0)
    run_id: int = 0


app = FastAPI(title="Self-Healing Links API")
app.mount("/web", StaticFiles(directory="web"), name="web")


@app.get("/")
def home() -> FileResponse:
    return FileResponse("web/index.html")


@app.get("/api/results")
def get_results(db_path: str = "dead_links.db", run_id: int = 0) -> Dict[str, Any]:
    database = DeadLinkDatabase(db_path=db_path)
    resolved_run_id = run_id if run_id > 0 else (database.latest_run_id() or 0)

    dead_links = database.get_dead_links(limit=500, run_id=resolved_run_id if resolved_run_id > 0 else None)
    suggestions = database.get_replacement_suggestions(
        limit=500,
        run_id=resolved_run_id if resolved_run_id > 0 else None,
    )
    classifications = database.get_classifications(
        limit=500,
        run_id=resolved_run_id if resolved_run_id > 0 else None,
    )

    return {
        "run_id": resolved_run_id,
        "stats": {
            "dead_links": len(dead_links),
            "suggestions": len(suggestions),
            "classifications": len(classifications),
            "auto_replace": sum(1 for row in classifications if row.get("decision") == "auto_replace"),
            "manual_review": sum(1 for row in classifications if row.get("decision") == "manual_review"),
        },
        "dead_links": dead_links,
        "replacement_suggestions": suggestions,
        "replacement_classifications": classifications,
    }


@app.post("/api/crawl")
async def run_crawl(payload: CrawlRequest) -> Dict[str, Any]:
    database = DeadLinkDatabase(db_path=payload.db_path)
    run_id = database.create_run(payload.target)
    crawler = LinkCrawler(database=database, max_depth=2, concurrency=10, run_id=run_id)

    try:
        dead_count = await crawler.crawl(payload.target)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    database.export_to_csv(payload.output_csv)
    return {
        "message": "crawl_complete",
        "run_id": run_id,
        "dead_links_found": dead_count,
        "db_path": payload.db_path,
        "csv": payload.output_csv,
    }


@app.post("/api/replace")
async def run_replace(payload: ReplaceRequest) -> Dict[str, Any]:
    database = DeadLinkDatabase(db_path=payload.db_path)
    resolved_run_id = payload.run_id if payload.run_id > 0 else (database.latest_run_id_with_dead_links() or 0)
    dead_links = database.get_dead_links(
        limit=payload.limit,
        run_id=resolved_run_id if resolved_run_id > 0 else None,
    )
    if not dead_links:
        raise HTTPException(
            status_code=400,
            detail=f"No dead links found for run_id={resolved_run_id}. Run crawl first.",
        )

    engine = ReplacementEngine(
        database=database,
        min_similarity=payload.min_similarity,
        top_k_per_link=payload.top_k,
    )
    suggestion_count = await engine.generate_replacements(
        dead_links=dead_links,
        run_id=resolved_run_id if resolved_run_id > 0 else None,
    )
    database.export_replacements_to_csv(payload.output_csv)

    return {
        "message": "replace_complete",
        "run_id": resolved_run_id,
        "suggestions_found": suggestion_count,
        "db_path": payload.db_path,
        "csv": payload.output_csv,
    }


@app.post("/api/classify")
async def run_classify(payload: ClassifyRequest) -> Dict[str, Any]:
    database = DeadLinkDatabase(db_path=payload.db_path)
    resolved_run_id = payload.run_id if payload.run_id > 0 else (database.latest_run_id_with_suggestions() or 0)
    suggestions = database.get_replacement_suggestions(
        limit=payload.limit,
        min_similarity=payload.min_similarity,
        run_id=resolved_run_id if resolved_run_id > 0 else None,
    )
    if not suggestions:
        raise HTTPException(
            status_code=400,
            detail=f"No replacement suggestions found for run_id={resolved_run_id}. Run replace first.",
        )

    classifier = SelfHealingClassifier(database=database, auto_threshold=payload.auto_threshold)
    classified_count, auto_count = await classifier.classify(
        suggestions=suggestions,
        run_id=resolved_run_id if resolved_run_id > 0 else None,
    )
    database.export_classifications_to_csv(payload.output_csv)

    return {
        "message": "classify_complete",
        "run_id": resolved_run_id,
        "classified": classified_count,
        "auto_replace": auto_count,
        "manual_review": classified_count - auto_count,
        "db_path": payload.db_path,
        "csv": payload.output_csv,
    }
