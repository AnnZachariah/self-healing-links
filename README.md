# Self-Healing Links

An ML-powered tool that automatically detects and replaces broken URLs (404s) on websites.

## Stage 1: Link Crawler (Complete)
- Crawls a URL recursively up to depth 2
- Detects dead links (404, 410, timeouts)
- Stores results in SQLite with anchor text and surrounding context
- Exports to CSV

## Usage
pip install -r requirements.txt
python main.py crawl https://yoursite.com

## Coming Soon
- Stage 2: Wayback Machine + semantic embeddings to find replacement URLs
- Stage 3: Confidence classifier for auto vs manual replacement
