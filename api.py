import asyncio
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


class ClassifyRequest(BaseModel):
    db_path: str = "dead_links.db"
    output_csv: str = "replacement_classifications.csv"
    limit: int = Field(default=500, ge=1)
    min_similarity: float = Field(default=0.0, ge=0.0, le=1.0)
    auto_threshold: float = Field(default=0.75, ge=0.0, le=1.0)


app = FastAPI(title="Self-Healing Links API")
app.mount("/web", StaticFiles(directory="web"), name="web")


@app.get("/")
def home() -> FileResponse:
    return FileResponse("web/index.html")


@app.get("/api/results")
def get_results(db_path: str = "dead_links.db") -> Dict[str, Any]:
    database = DeadLinkDatabase(db_path=db_path)
    dead_links = database.get_dead_links(limit=500)
    suggestions = database.get_replacement_suggestions(limit=500)
    classifications = database.get_classifications(limit=500)

    return {
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
def run_crawl(payload: CrawlRequest) -> Dict[str, Any]:
    database = DeadLinkDatabase(db_path=payload.db_path)
    crawler = LinkCrawler(database=database, max_depth=2, concurrency=10)

    try:
        dead_count = asyncio.run(crawler.crawl(payload.target))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    database.export_to_csv(payload.output_csv)
    return {
        "message": "crawl_complete",
        "dead_links_found": dead_count,
        "db_path": payload.db_path,
        "csv": payload.output_csv,
    }


@app.post("/api/replace")
def run_replace(payload: ReplaceRequest) -> Dict[str, Any]:
    database = DeadLinkDatabase(db_path=payload.db_path)
    dead_links = database.get_dead_links(limit=payload.limit)
    if not dead_links:
        raise HTTPException(status_code=400, detail="No dead links found. Run crawl first.")

    engine = ReplacementEngine(
        database=database,
        min_similarity=payload.min_similarity,
        top_k_per_link=payload.top_k,
    )
    suggestion_count = asyncio.run(engine.generate_replacements(dead_links=dead_links))
    database.export_replacements_to_csv(payload.output_csv)

    return {
        "message": "replace_complete",
        "suggestions_found": suggestion_count,
        "db_path": payload.db_path,
        "csv": payload.output_csv,
    }


@app.post("/api/classify")
def run_classify(payload: ClassifyRequest) -> Dict[str, Any]:
    database = DeadLinkDatabase(db_path=payload.db_path)
    suggestions = database.get_replacement_suggestions(
        limit=payload.limit,
        min_similarity=payload.min_similarity,
    )
    if not suggestions:
        raise HTTPException(status_code=400, detail="No replacement suggestions found. Run replace first.")

    classifier = SelfHealingClassifier(database=database, auto_threshold=payload.auto_threshold)
    classified_count, auto_count = asyncio.run(classifier.classify(suggestions=suggestions))
    database.export_classifications_to_csv(payload.output_csv)

    return {
        "message": "classify_complete",
        "classified": classified_count,
        "auto_replace": auto_count,
        "manual_review": classified_count - auto_count,
        "db_path": payload.db_path,
        "csv": payload.output_csv,
    }
