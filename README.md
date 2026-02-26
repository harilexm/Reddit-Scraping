# Reddit Roman Urdu Scraper

## The Problem

There is no large-scale, open dataset of **Roman Urdu** (Urdu written in Latin script) sourced from organic online conversations. Existing NLP resources focus on Nastaliq-script Urdu or English, leaving Roman Urdu — the way millions of Pakistanis actually type online — severely under-represented.

This project scrapes Reddit to build a Roman Urdu text corpus from Pakistani subreddit communities.

## How It Works

The pipeline has two stages:

### 1. Link Collection — `ScrapeLinks/`

Collects post URLs from **35+ Pakistani subreddits** (cities, universities, lifestyle, memes, etc.) using the Reddit JSON API.

- Paginates with `after` token across 4 sort orders: `controversial`, `top`, `new`, `hot`
- Auto-resumes on crash via checkpoint files
- Rate-limit aware with random delays

### 2. Comment Scraping — `CommentScraping(Fast)/`

Fetches comments from every collected link and filters for Roman Urdu.

- **3-phase approach**: batch-validate post IDs → fetch comments (`limit=500`) → fetch hidden "more children" comments
- **Async** with `aiohttp`, token-bucket rate limiter, and exponential backoff
- **Roman Urdu detection** via fastText negative filter + bigram matching + 200+ keyword scoring
- Rejects Nastaliq-script Urdu and non-Urdu Latin languages (French, Spanish, etc.)
- Auto-resume, live ETA, and periodic CSV checkpoints

> A simpler **synchronous** version lives in `CommentScraping(Slow)/` for reference.

## Project Structure

```
├── ScrapeLinks/
│   └── getLinks.py          # Link collector (JSON API)
├── CommentScraping(Fast)/
│   └── CommentScraper.py    # Async comment scraper + Roman Urdu filter
├── CommentScraping(Slow)/
│   └── CommentScraper.py    # Simple synchronous version
└── TestBrowserWorking/
    └── BrowserWorkingTest.py # Selenium sanity check
```

## Setup

```bash
python -m venv venv
venv\Scripts\activate
pip install pandas requests aiohttp nltk fast-langdetect
```

## Usage

```bash
# Step 1 — Collect links
python ScrapeLinks/getLinks.py

# Step 2 — Scrape comments
python CommentScraping(Fast)/CommentScraper.py
```

Both scripts auto-resume from where they left off if interrupted.

## Output Numbers

| Metric | Count |
|---|---|
| Subreddits targeted | 35+ |
| Post links collected | **~59,000** |
| Roman Urdu comments extracted | **~17,000** (deduplicated) |
| Output format | CSV (`text` column) |
