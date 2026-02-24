# Self-Healing Links: Workflow, Logic, and Example

## What Is Implemented

### Stage 1: Crawl & Dead Link Detection
- Input: website URL or `sitemap.xml`.
- Crawls links recursively up to depth `2`.
- Uses async `httpx` with concurrency control (`Semaphore`, default `10`).
- Checks links with `HEAD` (fallback to `GET` on `405`).
- Flags dead links on:
  - `404`
  - `410`
  - timeout
  - connection/request errors
- Stores dead links in SQLite (`dead_links` table).
- Exports `dead_links.csv`.

### Stage 2: Replacement Engine
- Reads dead links from SQLite.
- Uses Wayback APIs:
  - closest snapshot lookup
  - CDX candidate URL retrieval
- Ranks candidate replacements with:
  - text similarity (token-based cosine)
  - path similarity bonuses
  - penalties for noisy query URLs
- Stores suggestions in SQLite (`replacement_suggestions` table).
- Exports `replacement_suggestions.csv`.

### Stage 3: Confidence Classifier
- Reads replacement suggestions from SQLite.
- Uses a feature-based logistic confidence model.
- Outputs:
  - `confidence_score` (0 to 1)
  - `decision`: `auto_replace` or `manual_review`
  - `rationale` (top feature contributions)
- Stores classifications in SQLite (`replacement_classifications` table).
- Exports `replacement_classifications.csv`.

### Reviewer Decisions (Human-in-the-loop)
- Reviewer selects a suggestion and marks:
  - `approved`
  - `rejected`
  - `edited`
- Tracks:
  - reviewer name
  - optional note
  - decision timestamp
  - final URL (edited override)
- Stores in SQLite (`reviewer_decisions` table).

## Run Isolation Logic (`run_id`)

- Every crawl creates a new `run_id` in `crawl_runs`.
- All stage outputs are tagged with that `run_id`.
- `replace` and `classify` operate on:
  - explicit `run_id` if provided, or
  - latest run that has valid upstream data.
- UI keeps `currentRunId` in memory and sends it to API calls.
- Results view fetches only the selected/current run to avoid mixing old data.

## Data Flow

1. `crawl` writes `dead_links` for a run.
2. `replace` reads that run’s dead links and writes `replacement_suggestions`.
3. `classify` reads that run’s suggestions and writes `replacement_classifications`.
4. Reviewer decisions are captured per suggestion in `reviewer_decisions`.
5. UI/API results show dead links, suggestions, classifications, and reviewer decisions for one run.

## Core Tables

- `crawl_runs`
  - `id`, `target`, `started_at`
- `dead_links`
  - `id`, `run_id`, `source_page`, `dead_url`, `anchor_text`, `surrounding_context`, `status_code`, `discovered_at`
- `replacement_suggestions`
  - `id`, `run_id`, `source_page`, `dead_url`, `anchor_text`, `suggested_url`, `wayback_snapshot_url`, `similarity_score`, `match_reason`, `generated_at`
- `replacement_classifications`
  - `id`, `run_id`, `suggestion_id`, `dead_url`, `suggested_url`, `similarity_score`, `confidence_score`, `decision`, `rationale`, `classified_at`

## Commands (CLI)

```bash
python main.py crawl https://wiby.me
python main.py replace --top-k 5 --min-similarity 0.05
python main.py classify --auto-threshold 0.75
```

Optional explicit run scoping:

```bash
python main.py replace --run-id 3 --top-k 5 --min-similarity 0.05
python main.py classify --run-id 3 --auto-threshold 0.75
```

## API Endpoints

- `POST /api/crawl`
- `POST /api/replace`
- `POST /api/classify`
- `GET /api/results?run_id=<id>`

## Example (End-to-End)

### Input
- Crawl target: `https://wiby.me`

### Stage 1 Example Output
- Dead link found:
  - `https://wiby.me/about/url_that_was_submitted` (`404`)

### Stage 2 Example Output
- Top suggestions (example):
  - `https://wiby.me/about/` (`0.5074`)
  - `https://wiby.me/about/guide.html` (`0.4064`)
  - `http://wiby.me/about/pp.html` (`0.4064`)
  - `https://wiby.me/about.html` (`0.3775`)
  - `http://wiby.me/about/pp.org.html` (`0.3760`)

### Stage 3 Example Output
- Classifications (example):
  - `https://wiby.me/about/guide.html` -> `0.9436` -> `auto_replace`
  - `https://wiby.me/about/` -> `0.9220` -> `auto_replace`
  - `http://wiby.me/about/pp.html` -> `0.9183` -> `auto_replace`
  - `http://wiby.me/about/pp.org.html` -> `0.9183` -> `auto_replace`
  - `https://wiby.me/about.html` -> `0.8137` -> `auto_replace`

## Notes

- The app currently focuses on detection, suggestion, and classification.
- It is hosted-safe in current form (no direct file mutation workflow exposed).
- Historical rows are preserved in DB; `run_id` ensures clean per-run viewing.
