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

## Usage
- pip install -r requirements.txt
- python main.py crawl https://yoursite.com
- python main.py replace
- python main.py replace --top-k 5 --min-similarity 0.01

## Coming Soon
- Stage 3: Confidence classifier for auto vs manual replacement
