#!/usr/bin/env python3
"""TikTok -> YouTube Shorts pipeline.

Sources of videos (in this order):
  1. urls.txt          TikTok links you add by hand, one per line
  2. discovery         trending / search / creators, filtered by yt_config.json
                       (re-uses the tikwm discovery + download code from ig_pipeline.py)

Every video is downloaded without watermark, uploaded as a Short, and its id is
written to done.txt so it is never posted twice.

Usage:
    python pipeline.py --dry-run        # discover + download only, nothing is uploaded
    python pipeline.py                  # upload up to max_uploads_per_run Shorts
    python pipeline.py --limit 2        # override how many to upload this run
    python pipeline.py --manual-only    # only use urls.txt, skip discovery

Config: yt_config.json (optional, every key has a default, see DEFAULTS below).
"""
import argparse
import json
import os
import random
import re
import sys
import time

import requests
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload

import ig_pipeline as igp   # tikwm discovery/download helpers (also chdirs to this folder)

HERE = os.path.dirname(os.path.abspath(__file__))
os.chdir(HERE)

CONFIG_FILE = "yt_config.json"
URLS_FILE = "urls.txt"
DONE_FILE = "done.txt"
DOWNLOAD_DIR = "downloads"
SCOPES = ["https://www.googleapis.com/auth/youtube.upload"]

DEFAULTS = {
    # --- which videos get picked up (same meaning as in ig_config.json) ---
    "source": "trending",        # trending | search | users | none (none = only urls.txt)
    "region": "US",              # trending only
    "feed_calls": 3,
    "search_keywords": [],       # search only, e.g. ["funny cats"]
    "creators": [],              # users only, TikTok usernames without @
    "include_words": [],         # keep only videos whose title/hashtags contain one of these
    "exclude_words": [],         # drop videos containing any of these
    "min_seconds": 5,
    "max_seconds": 90,
    "min_likes": 50000,
    # --- YouTube side ---
    "max_uploads_per_run": 5,    # one upload costs 1600 quota units; default quota 10 000/day => max 6
    "privacy": "public",         # public | unlisted | private
    "category_id": "22",         # 22 = People & Blogs, 23 = Comedy, 15 = Pets & Animals, 24 = Entertainment
    "extra_tags": "#Shorts #fyp",
    "credit_original": True,     # adds "Credit: @author (TikTok)" to the description
}


# ------------------------------------------------------------------ helpers
def load_config():
    cfg = dict(DEFAULTS)
    if os.path.exists(CONFIG_FILE):
        cfg.update(json.load(open(CONFIG_FILE)))
    # discovery code from ig_pipeline expects these keys too
    for k, v in igp.DEFAULTS.items():
        cfg.setdefault(k, v)
    return cfg


def video_id(s):
    """TikTok id from a URL (or the string itself if it already is an id)."""
    m = re.search(r"/video/(\d+)", s)
    if m:
        return m.group(1)
    return s.strip().rstrip("/").split("/")[-1].split("?")[0]


def load_done():
    if not os.path.exists(DONE_FILE):
        return set()
    return {video_id(t) for t in open(DONE_FILE).read().split()}


def mark_done(vid):
    with open(DONE_FILE, "a") as f:
        f.write(vid + "\n")


def manual_videos():
    if not os.path.exists(URLS_FILE):
        return []
    out = []
    for line in open(URLS_FILE):
        url = line.strip()
        if url and not url.startswith("#"):
            out.append({"id": video_id(url), "url": url, "title": "", "author": "",
                        "likes": 0, "play": ""})
    return out


def clean_title(title, vid):
    t = re.sub(r"#\w+", "", title or "")          # hashtags go to the description
    t = re.sub(r"[<>]", "", t)                      # not allowed in YouTube titles
    t = re.sub(r"\s+", " ", t).strip()
    return (t or f"Video {vid}")[:90]


def build_description(cfg, v):
    parts = []
    if v["title"]:
        parts.append(v["title"][:4000])
    if cfg["credit_original"] and v["author"]:
        parts.append(f"Credit: @{v['author']} (TikTok)")
    if cfg["extra_tags"]:
        parts.append(cfg["extra_tags"])
    return "\n\n".join(parts)[:4900]


# ----------------------------------------------------------------- download
def download(v, path):
    if v.get("url"):                               # manual link -> local tiktokdl-api
        r = requests.get(igp.DL_API, params={"url": v["url"]}, timeout=30)
        r.raise_for_status()
        igp._save(r.json()["video"][0], path)
    else:                                          # discovered -> direct link, then local API
        igp.download(v, path)


# ------------------------------------------------------------------ youtube
def youtube():
    creds = None
    if os.path.exists("token.json") and os.path.getsize("token.json") > 0:
        try:
            creds = Credentials.from_authorized_user_file("token.json", SCOPES)
        except ValueError:
            print("token.json is invalid, logging in again", file=sys.stderr)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            try:
                flow = InstalledAppFlow.from_client_secrets_file("client_secret.json", SCOPES)
            except (ValueError, json.JSONDecodeError, OSError):
                sys.exit("client_secret.json is missing or is not the file downloaded from Google "
                         "Cloud (OAuth client ID, type Desktop app). See the setup steps.")
            creds = flow.run_local_server(port=0)
        open("token.json", "w").write(creds.to_json())
    return build("youtube", "v3", credentials=creds)


class QuotaExceeded(Exception):
    pass


def upload(yt, cfg, path, v):
    title = clean_title(v["title"], v["id"]) + " #Shorts"
    body = {
        "snippet": {
            "title": title,
            "description": build_description(cfg, v),
            "categoryId": str(cfg["category_id"]),
        },
        "status": {"privacyStatus": cfg["privacy"], "selfDeclaredMadeForKids": False},
    }
    media = MediaFileUpload(path, mimetype="video/mp4", chunksize=8 * 1024 * 1024, resumable=True)
    req = yt.videos().insert(part="snippet,status", body=body, media_body=media)

    response, retries = None, 0
    while response is None:
        try:
            _, response = req.next_chunk()
        except HttpError as e:
            content = (e.content or b"").decode("utf-8", "ignore")
            if "quotaExceeded" in content or "uploadLimitExceeded" in content:
                raise QuotaExceeded(content[:200])
            if e.resp.status in (500, 502, 503, 504) and retries < 5:
                retries += 1
                time.sleep(2 ** retries + random.random())
                continue
            raise
        except (requests.ConnectionError, TimeoutError, OSError):
            if retries >= 5:
                raise
            retries += 1
            time.sleep(2 ** retries + random.random())
    return response["id"]


# --------------------------------------------------------------------- main
def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dry-run", action="store_true", help="discover and download only, do not upload")
    ap.add_argument("--limit", type=int, default=None, help="max uploads this run")
    ap.add_argument("--manual-only", action="store_true", help="only use urls.txt, skip discovery")
    args = ap.parse_args()

    cfg = load_config()
    limit = args.limit if args.limit is not None else int(cfg["max_uploads_per_run"])
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)

    done = load_done()
    candidates = [v for v in manual_videos() if v["id"] not in done]
    if not args.manual_only and cfg["source"] != "none":
        seen = {v["id"] for v in candidates}
        candidates += [v for v in igp.discover(cfg) if v["id"] not in done and v["id"] not in seen]
    print(f"{len(candidates)} new candidates, uploading up to {limit}")
    if not candidates:
        return

    yt = None if args.dry_run else youtube()
    uploaded = failures = 0
    for v in candidates:
        if uploaded >= limit:
            break
        if failures >= 2:
            print("two failures in a row, stopping (check the error above)", file=sys.stderr)
            break
        path = os.path.join(DOWNLOAD_DIR, f"{v['id']}.mp4")
        try:
            print(f"- {v['id']} {('@' + v['author']) if v['author'] else v.get('url', '')}")
            download(v, path)
            if args.dry_run:
                print(f"  downloaded -> {path}")
            else:
                yt_id = upload(yt, cfg, path, v)
                mark_done(v["id"])
                os.remove(path)
                print(f"  uploaded https://youtube.com/shorts/{yt_id}")
            uploaded += 1
            failures = 0
        except QuotaExceeded as e:
            print(f"YouTube quota/upload limit reached, stopping: {e}", file=sys.stderr)
            break
        except Exception as e:
            failures += 1
            print(f"  failed: {e}", file=sys.stderr)


if __name__ == "__main__":
    main()
