# Self-Healing Links

An ML-powered tool that automatically detects and replaces broken URLs (404s) on websites.

## Stage 1: Link Crawler (Complete)
- Crawls a URL recursively up to depth 2
- Detects dead links (404, 410, timeouts)
- Stores results in SQLite with anchor text and surrounding context
- Exports to CSV

## Stage 2: Replacement Engine (Initial)
- Reads dead links from SQLite
- Uses Wayback Machine APIs to discover historical/candidate URLs
- Ranks candidate replacements using lightweight semantic similarity
- Stores suggestions in SQLite and exports to CSV

## Stage 3: Confidence Classifier (Initial)
- Classifies replacement suggestions into `auto_replace` or `manual_review`
- Uses a feature-based logistic confidence model (calibrated probabilities)
- Stores classifications in SQLite and exports to CSV

## Reviewer Decisions
- Reviewers can mark each suggestion as `approved`, `rejected`, or `edited`
- Tracks reviewer name, note, and decision timestamp
- Stores final edited URL when decision is `edited`

## Usage
- pip install -r requirements.txt
- python main.py crawl https://yoursite.com
- python main.py replace
- python main.py replace --top-k 5 --min-similarity 0.01
- python main.py classify --auto-threshold 0.75

Run isolation:
- Each crawl creates a `run_id`.
- Replace/Classify default to the latest run (or pass `--run-id` explicitly).

## Web App
- Start API + frontend server:
  - uvicorn api:app --reload
- Open in browser:
  - http://127.0.0.1:8000
- Use the UI buttons to run Stage 1, Stage 2, and Stage 3.
- Explainability: click any row in Replacement Suggestions or Classifications to open feature contributions and token/path match details.
- Reviewer decisions: after selecting a row, use Approve/Reject/Edit in the Explainability Panel.

## Coming Soon
- Model training pipeline from labeled replacement decisions
