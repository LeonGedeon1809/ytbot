# ytbot

Downloads trending TikTok videos (or links you provide) and reposts them as YouTube Shorts and Instagram Reels.

- `pipeline.py` - TikTok -> YouTube Shorts
- `ig_pipeline.py` - TikTok -> Instagram (Instagram Login API)

Reposting other people's content can lead to copyright claims or "reused content" strikes. Credit the original author and use at your own risk.

## Setup

```sh
python3 -m venv venv
source venv/bin/activate        # fish: source venv/bin/activate.fish
pip install -r requirements.txt
```

### YouTube
1. Create a project in Google Cloud and enable **YouTube Data API v3**.
2. Configure the OAuth consent screen and add your Google account as a test user.
3. Create an **OAuth client ID** of type **Desktop app**, download the JSON and save it as `client_secret.json` in this folder.
4. Run `python pipeline.py --limit 1` once on a machine with a browser. This creates `token.json`.

### Instagram
Copy `ig_config.example.json` to `ig_config.json` and fill in `ig_user_id` and `access_token`.

### Settings
Edit `yt_config.json` (source, topic filters, privacy, uploads per run).

## Usage

```sh
python pipeline.py --dry-run        # discover + download only
python pipeline.py                  # upload
python pipeline.py --manual-only    # only links from urls.txt
```

Manual links go in `urls.txt`, one per line. Posted video ids are recorded in `done.txt`.

## Secrets

`client_secret.json`, `token.json` and `ig_config.json` are listed in `.gitignore` and must never be committed.
