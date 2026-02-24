from typing import Any, Dict

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from classifier import SelfHealingClassifier
from crawler import LinkCrawler
from database import DeadLinkDatabase
from replacement_engine import ReplacementEngine
from apply_engine import ApplyEngine


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


class ReviewDecisionRequest(BaseModel):
    run_id: int = Field(..., ge=1)
    suggestion_id: int = Field(..., ge=1)
    decision: str
    reviewer_name: str = ""
    note: str = ""
    final_suggested_url: str = ""
    db_path: str = "dead_links.db"


class ApplyApprovedRequest(BaseModel):
    run_id: int = 0
    dry_run: bool = True
    connector: str = "none"
    files_root: str = "."
    limit: int = Field(default=500, ge=1)
    db_path: str = "dead_links.db"


app = FastAPI(title="Self-Healing Links API")
app.mount("/web", StaticFiles(directory="web"), name="web")


@app.get("/")
def home() -> FileResponse:
    return FileResponse("web/index.html")


@app.get("/api/results")
def get_results(db_path: str = "dead_links.db", run_id: int = 0) -> Dict[str, Any]:
    database = DeadLinkDatabase(db_path=db_path)
    resolved_run_id = run_id if run_id > 0 else (database.latest_run_id() or 0)
    run_started_at = database.get_run_started_at(resolved_run_id) if resolved_run_id > 0 else None

    dead_links = database.get_dead_links(limit=500, run_id=resolved_run_id if resolved_run_id > 0 else None)
    suggestions = database.get_replacement_suggestions(
        limit=500,
        run_id=resolved_run_id if resolved_run_id > 0 else None,
    )
    classifications = database.get_classifications(
        limit=500,
        run_id=resolved_run_id if resolved_run_id > 0 else None,
    )
    reviewer_decisions = database.get_reviewer_decisions(
        run_id=resolved_run_id if resolved_run_id > 0 else None,
        limit=500,
    )
    applied_replacements = database.get_applied_replacements(
        run_id=resolved_run_id if resolved_run_id > 0 else None,
        limit=500,
    )

    return {
        "run_id": resolved_run_id,
        "run_started_at": run_started_at,
        "stats": {
            "dead_links": len(dead_links),
            "suggestions": len(suggestions),
            "classifications": len(classifications),
            "auto_replace": sum(1 for row in classifications if row.get("decision") == "auto_replace"),
            "manual_review": sum(1 for row in classifications if row.get("decision") == "manual_review"),
            "review_approved": sum(1 for row in reviewer_decisions if row.get("decision") == "approved"),
            "review_rejected": sum(1 for row in reviewer_decisions if row.get("decision") == "rejected"),
            "review_edited": sum(1 for row in reviewer_decisions if row.get("decision") == "edited"),
            "apply_processed": sum(1 for _ in applied_replacements),
            "apply_dry_run": sum(1 for row in applied_replacements if row.get("status") == "dry_run"),
            "apply_applied": sum(1 for row in applied_replacements if row.get("status") == "applied"),
            "apply_skipped": sum(1 for row in applied_replacements if str(row.get("status", "")).startswith("skipped")),
            "apply_failed": sum(1 for row in applied_replacements if row.get("status") == "failed"),
        },
        "dead_links": dead_links,
        "replacement_suggestions": suggestions,
        "replacement_classifications": classifications,
        "reviewer_decisions": reviewer_decisions,
        "applied_replacements": applied_replacements,
    }


@app.post("/api/crawl")
async def run_crawl(payload: CrawlRequest) -> Dict[str, Any]:
    database = DeadLinkDatabase(db_path=payload.db_path)
    run_id = database.create_run(payload.target)
    run_started_at = database.get_run_started_at(run_id)
    crawler = LinkCrawler(database=database, max_depth=2, concurrency=10, run_id=run_id)

    try:
        dead_count = await crawler.crawl(payload.target)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    database.export_to_csv(payload.output_csv)
    return {
        "message": "crawl_complete",
        "run_id": run_id,
        "run_started_at": run_started_at,
        "dead_links_found": dead_count,
        "db_path": payload.db_path,
        "csv": payload.output_csv,
    }


@app.post("/api/replace")
async def run_replace(payload: ReplaceRequest) -> Dict[str, Any]:
    database = DeadLinkDatabase(db_path=payload.db_path)
    resolved_run_id = payload.run_id if payload.run_id > 0 else (database.latest_run_id_with_dead_links() or 0)
    run_started_at = database.get_run_started_at(resolved_run_id) if resolved_run_id > 0 else None
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
        "run_started_at": run_started_at,
        "suggestions_found": suggestion_count,
        "db_path": payload.db_path,
        "csv": payload.output_csv,
    }


@app.post("/api/classify")
async def run_classify(payload: ClassifyRequest) -> Dict[str, Any]:
    database = DeadLinkDatabase(db_path=payload.db_path)
    resolved_run_id = payload.run_id if payload.run_id > 0 else (database.latest_run_id_with_suggestions() or 0)
    run_started_at = database.get_run_started_at(resolved_run_id) if resolved_run_id > 0 else None
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
        "run_started_at": run_started_at,
        "classified": classified_count,
        "auto_replace": auto_count,
        "manual_review": classified_count - auto_count,
        "db_path": payload.db_path,
        "csv": payload.output_csv,
    }


@app.get("/api/explain/suggestion/{suggestion_id}")
def explain_suggestion(
    suggestion_id: int,
    db_path: str = "dead_links.db",
    run_id: int = 0,
) -> Dict[str, Any]:
    database = DeadLinkDatabase(db_path=db_path)
    resolved_run_id = run_id if run_id > 0 else None
    suggestion = database.get_replacement_suggestion_by_id(suggestion_id, run_id=resolved_run_id)
    if suggestion is None:
        raise HTTPException(status_code=404, detail=f"Suggestion not found: id={suggestion_id}")

    classifier = SelfHealingClassifier(database=database)
    explanation = classifier.explain_suggestion(suggestion)
    explanation["suggestion_id"] = suggestion_id
    explanation["run_id"] = suggestion.get("run_id")
    return explanation


@app.get("/api/explain/classification/{classification_id}")
def explain_classification(
    classification_id: int,
    db_path: str = "dead_links.db",
    run_id: int = 0,
) -> Dict[str, Any]:
    database = DeadLinkDatabase(db_path=db_path)
    resolved_run_id = run_id if run_id > 0 else None
    classification = database.get_classification_by_id(classification_id, run_id=resolved_run_id)
    if classification is None:
        raise HTTPException(status_code=404, detail=f"Classification not found: id={classification_id}")

    suggestion_id = int(classification.get("suggestion_id", 0))
    suggestion = database.get_replacement_suggestion_by_id(suggestion_id, run_id=resolved_run_id)
    if suggestion is None:
        raise HTTPException(
            status_code=404,
            detail=f"Suggestion not found for classification id={classification_id}, suggestion_id={suggestion_id}",
        )

    classifier = SelfHealingClassifier(database=database)
    explanation = classifier.explain_suggestion(suggestion)
    explanation["classification_id"] = classification_id
    explanation["suggestion_id"] = suggestion_id
    explanation["run_id"] = suggestion.get("run_id")
    return explanation


@app.post("/api/review/decision")
def save_review_decision(payload: ReviewDecisionRequest) -> Dict[str, Any]:
    database = DeadLinkDatabase(db_path=payload.db_path)
    run_id = payload.run_id
    suggestion_id = payload.suggestion_id

    suggestion = database.get_replacement_suggestion_by_id(suggestion_id, run_id=run_id)
    if suggestion is None:
        raise HTTPException(
            status_code=404,
            detail=f"Suggestion not found for run_id={run_id}, suggestion_id={suggestion_id}",
        )

    decision = payload.decision.strip().lower()
    if decision not in {"approved", "rejected", "edited"}:
        raise HTTPException(status_code=400, detail="Decision must be one of: approved, rejected, edited")

    original_suggested_url = str(suggestion.get("suggested_url", ""))
    final_suggested_url = payload.final_suggested_url.strip() or original_suggested_url

    database.upsert_reviewer_decision(
        run_id=run_id,
        suggestion_id=suggestion_id,
        dead_url=str(suggestion.get("dead_url", "")),
        original_suggested_url=original_suggested_url,
        final_suggested_url=final_suggested_url,
        decision=decision,
        reviewer_name=payload.reviewer_name.strip(),
        note=payload.note.strip(),
    )

    saved = database.get_reviewer_decision_by_suggestion(run_id=run_id, suggestion_id=suggestion_id)
    return {
        "message": "review_decision_saved",
        "run_id": run_id,
        "suggestion_id": suggestion_id,
        "decision": decision,
        "record": saved,
    }


@app.post("/api/apply-approved")
def apply_approved(payload: ApplyApprovedRequest) -> Dict[str, Any]:
    database = DeadLinkDatabase(db_path=payload.db_path)
    resolved_run_id = payload.run_id if payload.run_id > 0 else (database.latest_run_id() or 0)
    if resolved_run_id <= 0:
        raise HTTPException(status_code=400, detail="No run found. Run crawl first.")

    engine = ApplyEngine(database=database)
    summary = engine.apply_approved(
        run_id=resolved_run_id,
        dry_run=payload.dry_run,
        connector=payload.connector.strip() or "none",
        limit=payload.limit,
        files_root=payload.files_root or ".",
    )
    applied_replacements = database.get_applied_replacements(run_id=resolved_run_id, limit=500)
    return {
        "message": "apply_approved_complete",
        "run_id": resolved_run_id,
        "dry_run": payload.dry_run,
        "connector": payload.connector.strip() or "none",
        "files_root": payload.files_root or ".",
        "summary": summary,
        "applied_replacements": applied_replacements,
    }
