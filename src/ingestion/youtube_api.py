import json, os, time, urllib.parse, urllib.request, urllib.error

BASE = "https://www.googleapis.com/youtube/v3"

def _get(endpoint, params, retries=3):
    url = f"{BASE}/{endpoint}?{urllib.parse.urlencode(params)}"
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(url, timeout=30) as resp:
                return json.load(resp)
        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8", "ignore")[:300]
            if e.code >= 500 and attempt < retries - 1:
                time.sleep(2 ** attempt); continue
            raise RuntimeError(f"YouTube API HTTP {e.code}: {body}") from None   # never echo the URL (it holds the key)
        except urllib.error.URLError as e:
            if attempt < retries - 1:
                time.sleep(2 ** attempt); continue
            raise RuntimeError(f"Network error: {e.reason}") from None

def fetch_trending(api_key, region, max_pages=1, page_size=50):
    """Current mostPopular chart for a region (1 quota unit per page)."""
    items, token = [], None
    for _ in range(max_pages):
        params = {"part": "snippet,contentDetails,statistics", "chart": "mostPopular",
                  "regionCode": region, "maxResults": page_size, "key": api_key}
        if token:
            params["pageToken"] = token
        data = _get("videos", params)
        items += data.get("items", [])
        token = data.get("nextPageToken")
        if not token:
            break
    return items

def fetch_categories(api_key, region="US"):
    data = _get("videoCategories", {"part": "snippet", "regionCode": region, "key": api_key})
    return [(int(i["id"]), i["snippet"]["title"]) for i in data.get("items", [])]

def write_jsonl(items, path):
    """One unmodified video item per line, the format Bronze expects."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for it in items:
            f.write(json.dumps(it, ensure_ascii=False) + "\n")