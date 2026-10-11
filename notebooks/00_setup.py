spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.youtube_trending")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.youtube_trending.raw")


import os, re, json, glob, csv

repo = os.path.abspath("..")   
vol  = "/Volumes/workspace/youtube_trending/raw"
name_re = re.compile(r"^(?:full_load|incremental)_([A-Z]{2})_(\d{4}-\d{2}-\d{2})\.json$")

def to_items(obj):
    if isinstance(obj, dict) and "items" in obj:    
        return obj["items"]
    if isinstance(obj, list):                        
        return obj
    raise ValueError("Unrecognised JSON shape")

for folder in ("full_load", "incremental"):
    for path in sorted(glob.glob(f"{repo}/data/samples/{folder}/*.json")):
        m = name_re.match(os.path.basename(path))
        if not m:
            print("skipped (name doesn't match):", os.path.basename(path)); continue
        region, d = m.groups()
        with open(path, encoding="utf-8") as fh:
            items = to_items(json.load(fh))
        out_dir = f"{vol}/youtube_api/ingestion_date={d}/region={region}"
        os.makedirs(out_dir, exist_ok=True)
        with open(f"{out_dir}/videos.jsonl", "w", encoding="utf-8") as out:
            for it in items:
                out.write(json.dumps(it, ensure_ascii=False) + "\n")
        print(f"{folder:12} {region} {d}  {len(items)} items")


cats = [(1,"Film & Animation"),(2,"Autos & Vehicles"),(10,"Music"),(15,"Pets & Animals"),
        (17,"Sports"),(19,"Travel & Events"),(20,"Gaming"),(22,"People & Blogs"),
        (23,"Comedy"),(24,"Entertainment"),(25,"News & Politics"),(26,"Howto & Style"),
        (27,"Education"),(28,"Science & Technology"),(29,"Nonprofits & Activism"),
        (30,"Movies"),(43,"Shows"),(44,"Trailers")]

os.makedirs(f"{vol}/reference", exist_ok=True)
with open(f"{vol}/reference/categories.csv", "w", newline="", encoding="utf-8") as fh:
    w = csv.writer(fh); w.writerow(["category_id", "category_name"]); w.writerows(cats)