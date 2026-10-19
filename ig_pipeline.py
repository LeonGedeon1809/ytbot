#!/usr/bin/env python3
"""Trending TikTok -> Instagram Reels (Instagram API with Instagram Login, no Facebook).

Flow: fetch trending feed -> filter/rank -> download (no watermark) -> create an
Instagram Reels container -> upload the video -> wait for processing -> publish.

Two ways to get the video to Instagram (config "upload_mode"):
  "resumable"  send the bytes directly (no public hosting). Meta's docs list this
               under the Facebook Login host only, so it may be rejected here.
  "video_url"  Instagram fetches the file from a public HTTPS URL. The script serves
               the downloads folder on a local port for the duration of the run; point
               "public_base_url" at it (cloudflared/ngrok tunnel or port forward).
  "auto"       try resumable first, fall back to video_url if public_base_url is set.

Usage:
    python ig_pipeline.py --dry-run          # discover + download only, nothing is posted
    python ig_pipeline.py                    # post up to max_posts_per_run Reels
    python ig_pipeline.py --limit 1          # override how many to post this run
    python ig_pipeline.py --refresh-token    # extend the 60-day token, saves to ig_config.json
"""
import argparse
import functools
import http.server
import json
import os
import sys
import threading
import time

import requests

HERE = os.path.dirname(os.path.abspath(__file__))
os.chdir(HERE)

CONFIG_FILE = "ig_config.json"
POSTED_FILE = "ig_posted.txt"       # TikTok video ids already handled
DOWNLOAD_DIR = "downloads"

# Local downloader you already run (used as a fallback if the direct link fails)
DL_API = "http://localhost:3000/tiktok/api.php"
# Unofficial trending feed. Not guaranteed to stay up; swap discover() if it breaks.
FEED_API = "https://www.tikwm.com/api/feed/list"

GRAPH_VERSION = "v25.0"
GRAPH = f"https://graph.instagram.com/{GRAPH_VERSION}"
RUPLOAD = f"https://rupload.facebook.com/ig-api-upload/{GRAPH_VERSION}"

UA = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"}

DEFAULTS = {
    "ig_user_id": "",
    "access_token": "",
    "upload_mode": "video_url",  # video_url | resumable | auto (resumable is rejected by Instagram Login)
    "public_base_url": "",      # e.g. https://abc123.trycloudflare.com (no trailing slash)
    "serve_port": 8765,         # local port the script serves downloads/ on
    "source": "trending",     # trending | search | users  (which videos get picked up)
    "region": "SK",             # trending only: country code (e.g. US, GB, SK)
    "feed_calls": 3,            # trending only: how many times to ask the feed (each call returns a different batch)
    "search_keywords": [],      # search only: e.g. ["funny cats", "football"]
    "creators": [],             # users only: TikTok usernames without @
    "include_words": [],        # keep only videos whose title/hashtags contain one of these
    "exclude_words": [],        # drop videos whose title/hashtags contain any of these
    "max_posts_per_run": 3,     # Instagram API allows 100 posts / 24 h; stay far below
    "min_seconds": 5,
    "max_seconds": 90,
    "min_likes": 50000,
    "caption_tags": "#fyp #trending #viral",
    "credit_original": True,    # appends "Credit: @author" to the caption
}


def load_config():
    cfg = dict(DEFAULTS)
    if os.path.exists(CONFIG_FILE):
        cfg.update(json.load(open(CONFIG_FILE)))
    cfg["ig_user_id"] = os.environ.get("IG_USER_ID", cfg["ig_user_id"])
    cfg["access_token"] = os.environ.get("IG_ACCESS_TOKEN", cfg["access_token"])
    cfg["public_base_url"] = cfg["public_base_url"].rstrip("/")
    return cfg


def load_posted():
    if not os.path.exists(POSTED_FILE):
        return set()
    return set(open(POSTED_FILE).read().split())


def mark_posted(video_id):
    with open(POSTED_FILE, "a") as f:
        f.write(video_id + "\n")


# ----------------------------------------------------------------- discovery
SEARCH_API = "https://www.tikwm.com/api/feed/search"
USER_API = "https://www.tikwm.com/api/user/posts"
TIKWM_HEADERS = {**UA, "Referer": "https://www.tikwm.com/", "Origin": "https://www.tikwm.com",
                 "X-Requested-With": "XMLHttpRequest", "Accept": "application/json, */*"}


def _tikwm_videos(method, url, label, **kw):
    """One unofficial-API call; report problems instead of crashing the whole run."""
    try:
        r = requests.request(method, url, headers=TIKWM_HEADERS, timeout=30, **kw)
        if not r.ok:
            print(f"  {label}: HTTP {r.status_code} {r.text[:150]!r}", file=sys.stderr)
            return []
        j = r.json()
        if j.get("code") not in (0, None):
            print(f"  {label}: API said {j.get('msg')!r}", file=sys.stderr)
            return []
        return (j.get("data") or {}).get("videos") or []
    except Exception as e:
        print(f"  {label}: {e}", file=sys.stderr)
        return []


def _get_retry(url, tries=4, **kw):
    """GET with retries/backoff; tikwm often times out or answers 5xx/429 for a while."""
    last = None
    for n in range(tries):
        try:
            r = requests.get(url, **kw)
            if r.status_code == 429 or r.status_code >= 500:
                raise requests.HTTPError(f"HTTP {r.status_code}")
            r.raise_for_status()
            return r
        except Exception as e:
            last = e
            if n + 1 < tries:
                wait = 5 * 2 ** n
                print(f"  attempt {n + 1}/{tries} failed ({e}); retrying in {wait}s", file=sys.stderr)
                time.sleep(wait)
    raise last


def _fetch(cfg):
    """Raw video dicts from the chosen source. All endpoints are unofficial."""
    src = cfg["source"]
    items = []
    if src == "trending":
        calls = max(1, int(cfg["feed_calls"]))
        for i in range(calls):
            try:
                r = _get_retry(FEED_API, params={"region": cfg["region"], "count": 30},
                               headers=UA, timeout=45)
                items += r.json().get("data") or []
            except Exception as e:
                print(f"  feed call {i + 1}/{calls} failed: {e}", file=sys.stderr)
                break              # tikwm is throttling or down; use what we already have
            if i + 1 < calls:
                time.sleep(3)
    elif src == "search":
        for kw in cfg["search_keywords"]:
            items += _tikwm_videos("post", SEARCH_API, f"search '{kw}'", data={
                "keywords": kw, "count": 30, "cursor": 0, "web": 1, "hd": 1})
            time.sleep(1.5)            # the endpoint rate-limits to roughly 1 request/s
    elif src == "users":
        for name in cfg["creators"]:
            items += _tikwm_videos("get", USER_API, f"creator '{name}'", params={
                "unique_id": name.lstrip("@"), "count": 30, "cursor": 0})
            time.sleep(1.5)
    else:
        sys.exit(f"unknown source {src!r} (use trending, search or users)")
    return items


def discover(cfg):
    """Return a list of trending video dicts, best first."""
    data = _fetch(cfg)
    include = [w.lower().lstrip("#") for w in cfg["include_words"]]
    exclude = [w.lower().lstrip("#") for w in cfg["exclude_words"]]
    out = []
    drops = {"length": 0, "likes": 0, "include_words": 0, "exclude_words": 0}
    for v in data:
        dur = v.get("duration", 0)
        likes = v.get("digg_count", 0)
        text = (v.get("title") or "").lower()
        if not (cfg["min_seconds"] <= dur <= cfg["max_seconds"]):
            drops["length"] += 1
            continue
        if likes < cfg["min_likes"]:
            drops["likes"] += 1
            continue
        if include and not any(w in text for w in include):
            drops["include_words"] += 1
            continue
        if any(w in text for w in exclude):
            drops["exclude_words"] += 1
            continue
        out.append({
            "id": str(v.get("video_id") or v.get("id")),
            "title": (v.get("title") or "").strip(),
            "author": (v.get("author") or {}).get("unique_id", ""),
            "likes": likes,
            "play": v.get("play", ""),
        })
    out.sort(key=lambda x: x["likes"], reverse=True)
    seen, unique = set(), []
    for v in out:                      # the feed repeats videos
        if v["id"] not in seen:
            seen.add(v["id"])
            unique.append(v)
    print(f"feed returned {len(data)} videos; kept {len(unique)}; dropped by: "
          + ", ".join(f"{k} {n}" for k, n in drops.items()))
    return unique


# ----------------------------------------------------------------- download
def _save(url, path):
    if url.startswith("/"):
        url = "https://www.tikwm.com" + url
    with requests.get(url, headers=UA, stream=True, timeout=120) as r:
        r.raise_for_status()
        with open(path, "wb") as f:
            for chunk in r.iter_content(1 << 16):
                f.write(chunk)
    if os.path.getsize(path) < 10_000:
        raise RuntimeError("downloaded file is suspiciously small")


def download(v, path):
    """Try the direct no-watermark link, then your local tiktokdl-api."""
    try:
        if v["play"]:
            _save(v["play"], path)
            return
    except Exception as e:
        print(f"  direct link failed ({e}), trying local tiktokdl-api", file=sys.stderr)
    page = f"https://www.tiktok.com/@{v['author']}/video/{v['id']}"
    r = requests.get(DL_API, params={"url": page}, timeout=30)
    r.raise_for_status()
    _save(r.json()["video"][0], path)


# ------------------------------------------------- local file server (video_url)
class _FileOnlyHandler(http.server.SimpleHTTPRequestHandler):
    def list_directory(self, path):          # never expose a folder listing
        self.send_error(404)
        return None

    def log_message(self, *a):               # keep the console quiet
        pass


def start_file_server(port):
    handler = functools.partial(_FileOnlyHandler, directory=os.path.abspath(DOWNLOAD_DIR))
    srv = http.server.ThreadingHTTPServer(("0.0.0.0", port), handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv


# ---------------------------------------------------------------- instagram
def _check(r):
    if not r.ok:
        raise RuntimeError(f"HTTP {r.status_code}: {r.text[:500]}")
    return r


def build_caption(cfg, v):
    parts = [v["title"][:1800]]
    if cfg["credit_original"] and v["author"]:
        parts.append(f"Credit: @{v['author']} (TikTok)")
    if cfg["caption_tags"]:
        parts.append(cfg["caption_tags"])
    return "\n\n".join(p for p in parts if p)[:2200]


def _container_resumable(cfg, path, caption):
    token, uid = cfg["access_token"], cfg["ig_user_id"]
    r = _check(requests.post(f"{GRAPH}/{uid}/media", data={
        "media_type": "REELS",
        "upload_type": "resumable",
        "caption": caption,
        "access_token": token,
    }, timeout=60))
    j = r.json()
    container = j["id"]
    upload_uri = j.get("uri") or f"{RUPLOAD}/{container}"
    with open(path, "rb") as f:
        _check(requests.post(upload_uri, headers={
            "Authorization": f"OAuth {token}",
            "offset": "0",
            "file_size": str(os.path.getsize(path)),
        }, data=f, timeout=900))
    return container


def _container_video_url(cfg, video_id, caption):
    if not cfg["public_base_url"]:
        raise RuntimeError("upload_mode needs public_base_url (see ig_config.example.json)")
    r = _check(requests.post(f"{GRAPH}/{cfg['ig_user_id']}/media", data={
        "media_type": "REELS",
        "video_url": f"{cfg['public_base_url']}/{video_id}.mp4",
        "caption": caption,
        "access_token": cfg["access_token"],
    }, timeout=60))
    return r.json()["id"]


def _wait_finished(cfg, container):
    for _ in range(72):  # ~6 minutes
        s = _check(requests.get(f"{GRAPH}/{container}", params={
            "fields": "status_code,status", "access_token": cfg["access_token"]},
            timeout=30)).json()
        code = s.get("status_code")
        if code == "FINISHED":
            return
        if code in ("ERROR", "EXPIRED"):
            raise RuntimeError(f"processing failed: {s}")
        time.sleep(5)
    raise TimeoutError("Instagram did not finish processing in time")


def ig_publish_reel(cfg, path, video_id, caption):
    mode = cfg["upload_mode"]
    container = None
    if mode in ("auto", "resumable"):
        try:
            container = _container_resumable(cfg, path, caption)
        except Exception as e:
            if mode == "resumable" or not cfg["public_base_url"]:
                raise
            print(f"  resumable upload failed ({e}); falling back to video_url", file=sys.stderr)
    if container is None:
        container = _container_video_url(cfg, video_id, caption)

    _wait_finished(cfg, container)
    r = _check(requests.post(f"{GRAPH}/{cfg['ig_user_id']}/media_publish", data={
        "creation_id": container, "access_token": cfg["access_token"]}, timeout=60))
    return r.json().get("id")


def refresh_token(cfg):
    """Extend a long-lived Instagram token (must be >24 h old and not yet expired)."""
    r = _check(requests.get("https://graph.instagram.com/refresh_access_token", params={
        "grant_type": "ig_refresh_token", "access_token": cfg["access_token"]}, timeout=30))
    j = r.json()
    if not os.path.exists(CONFIG_FILE):
        sys.exit(f"New token (put it in {CONFIG_FILE}): {j['access_token']}")
    data = json.load(open(CONFIG_FILE))
    data["access_token"] = j["access_token"]
    json.dump(data, open(CONFIG_FILE, "w"), indent=2)
    print(f"Token refreshed, valid for ~{j.get('expires_in', 0) // 86400} days.")


# --------------------------------------------------------------------- main
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true",
                    help="discover and download only, do not post")
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--refresh-token", action="store_true")
    args = ap.parse_args()

    cfg = load_config()
    if args.refresh_token:
        return refresh_token(cfg)
    if not args.dry_run and not (cfg["ig_user_id"] and cfg["access_token"]):
        sys.exit("Missing ig_user_id / access_token (see ig_config.example.json).")

    limit = args.limit if args.limit is not None else cfg["max_posts_per_run"]
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)

    if not args.dry_run and cfg["upload_mode"] in ("auto", "video_url") and cfg["public_base_url"]:
        start_file_server(int(cfg["serve_port"]))
        print(f"serving {DOWNLOAD_DIR}/ on port {cfg['serve_port']} -> {cfg['public_base_url']}")

    posted = load_posted()
    candidates = [v for v in discover(cfg) if v["id"] not in posted]
    print(f"{len(candidates)} new candidates, posting up to {limit}")

    done = 0
    failures = 0
    for v in candidates:
        if done >= limit:
            break
        if failures >= 2:
            print("two failures in a row, stopping (check the error above)", file=sys.stderr)
            break
        path = os.path.join(DOWNLOAD_DIR, f"{v['id']}.mp4")
        try:
            print(f"- {v['id']} @{v['author']} ({v['likes']} likes)")
            download(v, path)
            if args.dry_run:
                print(f"  downloaded -> {path}")
            else:
                media_id = ig_publish_reel(cfg, path, v["id"], build_caption(cfg, v))
                mark_posted(v["id"])
                os.remove(path)
                print(f"  published, media id {media_id}")
            done += 1
            failures = 0
        except Exception as e:
            failures += 1
            print(f"  failed: {e}", file=sys.stderr)


if __name__ == "__main__":
    main()
