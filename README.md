# YouTube Trending Analytics — End-to-End Medallion Lakehouse Pipeline

## What this is
Analyzes daily YouTube trending video activity across ~10 countries/regions to
understand which categories dominate trending charts, how fast engagement grows,
and how long videos stay trending — built as a Bronze/Silver/Gold pipeline.

## Repo structure
```
data/samples/full_load     -> initial API snapshot (JSON) + Kaggle historical CSVs
data/samples/incremental   -> follow-up daily snapshot (JSON), 24h after full load
notebooks/                 -> Bronze -> Silver -> Gold processing notebooks
docs/                      -> proposal and supporting documentation
scripts/                   -> helper scripts (e.g. YouTube API pull)
```

## Data source
- YouTube Data API v3, `videos.list` endpoint, `chart=mostPopular`
- Kaggle "Trending YouTube Video Statistics" (historical backfill)

## Setup
1. Get a YouTube Data API v3 key (Google Cloud Console).
2. Copy `.env.example` to `.env` and add your key. Never commit `.env`.
3. `pip install -r requirements.txt`
4. Run `python scripts/fetch_youtube_data.py --mode full` for the full load sample.
5. 24 hours later, run `python scripts/fetch_youtube_data.py --mode incremental`.

## Pipeline
- **Bronze**: raw JSON landed as-is, partitioned by `ingestion_date` and `region`.
- **Silver**: flattened, typed, deduplicated, category names mapped, PII handled
  (channelId hashed, description/tags redacted for emails/handles).
- **Gold**: star schema — `dim_video`, `dim_channel`, `dim_category`, `dim_region`,
  `dim_date`, `fact_video_daily_stats`, plus rollups `daily_category_trends` and
  `region_trending_summary`.

## Dashboard
Power BI, three views: trending category mix by region (stacked bar), engagement
velocity over consecutive snapshot days (line), and cross-region category overlap
(heatmap).
