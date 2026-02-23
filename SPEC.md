# Self-Healing Link Verification System

## Stage 1: Crawler & Detector
- Accept a URL or sitemap.xml as input
- Crawl all links found on the page recursively up to depth 2
- For each link, send an HTTP request and record the status code
- Flag any link returning 404, 410, or connection timeout as "dead"
- Store results in SQLite with these fields:
  - id, source_page, dead_url, anchor_text, surrounding_context, status_code, discovered_at
- surrounding_context = the paragraph or sentence the link was found in
- Print a live progress table in the terminal using rich
- Export results to dead_links.csv when done

## Stage 2: Replacement Engine (coming later)
## Stage 3: Self-Healing Classifier (coming later)