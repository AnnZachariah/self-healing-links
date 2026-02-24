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
- Uses a confidence score from similarity + URL/path quality heuristics
- Stores classifications in SQLite and exports to CSV

## Usage
- pip install -r requirements.txt
- python main.py crawl https://yoursite.com
- python main.py replace
- python main.py replace --top-k 5 --min-similarity 0.01
- python main.py classify --auto-threshold 0.75

## Web App
- Start API + frontend server:
  - uvicorn api:app --reload
- Open in browser:
  - http://127.0.0.1:8000
- Use the UI buttons to run Stage 1, Stage 2, and Stage 3 and inspect outputs.

## Coming Soon
- Calibrated ML confidence model (beyond heuristic scoring)
