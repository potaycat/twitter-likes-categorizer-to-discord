# X Likes → Gemini Decision 1 → Discord Thread Router

A single-file (`app.py`) real-time webhook listener and archive importer that watches your newly liked posts on **X (Twitter)**, fetches full post text and media via **BetterTwitFix (`api.vxtwitter.com`)**, classifies both text and visual media in a single pass using Google's **`gemini-decision-1`** (`v1beta` Decisions API), and posts rich embeds (multi-image galleries + inline playable videos) into category-specific Discord threads.

---

## How It Works

```text
X Activity API (like.create) ──┐
                               ├──► SQLite Dedup ──► BetterTwitFix API ──► gemini-decision-1 ──► Discord Thread Webhook
X Archive (data/like.js)     ──┘     (likes.db)     (text + media URLs)   (text + ≤4 images)     (?thread_id=...)
```

### Categories
Every liked post is classified into one of 6 Discord threads inside a single parent channel:

| Category Label | Env Variable | Description |
| :--- | :--- | :--- |
| `sfw-furry-art` | `THREAD_ID_SFW_FURRY` | Safe-for-work anthropomorphic/furry art, fursuits, or character designs. |
| `nsfw-furry-art` | `THREAD_ID_NSFW` | Adult, sexually suggestive, or explicit anthropomorphic/furry art/media. |
| `meme` | `THREAD_ID_MEME` | General non-furry jokes, shitposts, reaction images/videos, or memes. |
| `furry-meme` | `THREAD_ID_FURRY_MEME` | Furry or anthropomorphic jokes, shitposts, reaction images/videos, comics, or memes. |
| `tech` | `THREAD_ID_TECH` | Technology news, software, hardware, programming, AI, gadgets, cybersecurity, science, or engineering. |
| `other` | `THREAD_ID_OTHER` | General news, commentary, general photography, or anything not covered above; also low-confidence fallbacks. |

If model confidence is below `CONFIDENCE_THRESHOLD` (default `0.55`) or media fails to download, the post safely falls back to `other`.

---

## 1. Prerequisites & Credentials Setup

### A. Discord Webhook & Thread IDs
1. Create a parent text channel in your Discord server (e.g., `#x-likes`).
2. Inside `#x-likes`, create **6 threads** (one for each category above).
   * *Note: Mark NSFW threads/channel age-restricted as appropriate. Even if Discord auto-archives inactive threads later, posting via webhook automatically unarchives them.*
3. Create **one** channel webhook:
   * Right-click the parent channel → **Edit Channel** → **Integrations** → **Webhooks** → **New Webhook** → **Copy Webhook URL** → save as `DISCORD_WEBHOOK_URL`.
4. Copy the **6 Thread IDs**:
   * Open Discord **User Settings** → **Advanced** → turn on **Developer Mode**.
   * Right-click each thread in the channel sidebar → **Copy Thread ID** → fill in `THREAD_ID_*` in `.env`.

### B. Google Gemini API Key
1. Generate an API key in [Google AI Studio](https://aistudio.google.com/).
2. Set `GEMINI_API_KEY` in `.env`.

### C. X (Twitter) Developer Credentials
*(Only required for real-time `serve` + `subscribe`. Not needed if you only want to run `import` on `like.js`.)*
1. In the [X Developer Portal](https://developer.x.com/), open your App (with X Activity API access).
2. Under **User authentication settings**, ensure OAuth 1.0a is enabled with Read permissions.
3. Under **Keys and tokens**:
   * Copy **API Key** and **API Key Secret** → `X_CONSUMER_KEY` and `X_CONSUMER_SECRET`.
   * Generate **Access Token and Secret** for **your own X account** → `X_ACCESS_TOKEN` and `X_ACCESS_TOKEN_SECRET`.
   * *(Optional)* Copy **Bearer Token** → `X_BEARER_TOKEN`.
4. Set `X_USER_ID` to your numeric X account ID (the prefix before the `-` in your `X_ACCESS_TOKEN` is your numeric user ID, e.g., `1234567890-AbCdEf...` → `1234567890`).

---

## 2. Installation & Configuration

```bash
python3 -m venv .venv
source .venv/bin/activate

pip install -r requirements.in
cp .env.example .env
```

Edit `.env` and fill in your keys and Discord IDs.

---

## 3. Deploying to Fly.io (Fly Proxy HTTPS)

The included Dockerfile runs the webhook server with Python 3.14 on port `8000`. Fly Proxy provides the public HTTPS endpoint and forwards requests to it; no `cloudflared` tunnel or separate reverse proxy is needed.

1. Install [`flyctl`](https://fly.io/docs/flyctl/install/) and sign in with `fly auth login`.
2. Choose a globally unique Fly app name. Replace `replace-with-unique-fly-app-name` in both the `app` and `PUBLIC_WEBHOOK_URL` entries in `fly.toml`. The URL must be `https://<app-name>.fly.dev/webhook/x`.
3. Create the Fly app and its persistent volume in the same region. The config defaults to Singapore (`sin`); change `primary_region` and the volume's `--region` together if you prefer another region:
   ```bash
   fly apps create <app-name>
   fly volumes create likes_data --size 1 --region sin
   ```
5. Deploy and check the public endpoints:
   ```bash
   fly deploy
   curl "https://<app-name>.fly.dev/healthz"
   curl "https://<app-name>.fly.dev/webhook/x?crc_token=test"
   ```
   The health check should return `{"status":"ok",...}` and the CRC endpoint should return HTTP 200 with a `response_token`.

The volume stores `/data/likes.db` across deploys and restarts. This app's in-memory queue and SQLite database are intended for one machine; don't scale it to multiple machines without adding shared/replicated storage and queueing. Keep the machine running so webhook requests are handled without cold-start delays.

---

## 4. Running Real-Time Webhook Mode (Exact Order Matters!)

> **Important:** Always start `serve` **before** running `subscribe`. During `subscribe`, X immediately sends a live `GET /webhook/x?crc_token=...` verification request to your server.

### Step 1 — Ensure the Webhook Server Is Running
On Fly, the server starts with `fly deploy` and remains available through Fly Proxy. For local hosting, start it with:
```bash
python app.py serve
```

Verify the Fly public HTTPS URL reaches `app.py`:
```bash
curl "https://<app-name>.fly.dev/webhook/x?crc_token=test"
# Expected HTTP 200: {"response_token":"sha256=..."}

curl "https://<app-name>.fly.dev/healthz"
# Expected HTTP 200: {"status":"ok","queued_likes":0}
```

### Step 2 — Register Webhook & Subscribe on X (One-Time Setup)
Set `PUBLIC_WEBHOOK_URL=https://<app-name>.fly.dev/webhook/x` in your local `.env`, then run this from the project directory:
```bash
python app.py subscribe
```
X will send the CRC check through Fly Proxy to the running server. The command will report that your webhook and `like.create` outbound subscription are active.

**You do not need to run `subscribe` again** on normal restarts. Only re-run it if you change `PUBLIC_WEBHOOK_URL` or if X marks your webhook invalid after extended server downtime.

---

## 5. Tracking Multiple X Accounts (2+ Accounts)

You can route likes from **2 or more X accounts** into the same `PUBLIC_WEBHOOK_URL`, `app.py serve` instance, and Discord threads (if both accounts like the same post, `likes.db` automatically prevents duplicate Discord posts):

1. **Use the SAME X Developer App (`X_CONSUMER_KEY` & `X_CONSUMER_SECRET`) for all accounts:**
   * `app.py serve` verifies webhook signatures with a single `X_CONSUMER_SECRET`, so every subscribed account must authorize the **same X App**.
   * **Account 1 (App owner):** Generate `X_ACCESS_TOKEN` and `X_ACCESS_TOKEN_SECRET` directly in the X Developer Portal.
   * **Account 2+:** Authorize your App via 3-legged OAuth 1.0a (e.g., `twurl authorize --consumer-key "$X_CONSUMER_KEY" --consumer-secret "$X_CONSUMER_SECRET"`) while logged into Account 2 to obtain Account 2's `X_ACCESS_TOKEN`, `X_ACCESS_TOKEN_SECRET`, and `X_USER_ID`.

2. **Leave `X_USER_ID=` empty in `.env` when running `serve`:**
   * When `X_USER_ID` is set during `serve`, `app.py` filters out events from any other user ID.
   * Leaving `X_USER_ID=` blank in `.env` lets `serve` accept events from **all** of your subscribed accounts (outbound-only filtering is still enforced on X's side via `"direction": "outbound"`).

3. **Run `subscribe` once per account using inline environment variables:**
   * Keep `python app.py serve` running in Terminal 1 (with `X_USER_ID=` blank in `.env`).
   * In Terminal 2, subscribe each account (`app.py subscribe` automatically reuses your existing `webhook_id` and adds each new subscription alongside the previous ones, provided your X API tier supports multiple subscriptions):

   ```bash
   # Subscribe Account 1
   X_USER_ID="111111111" \
   X_ACCESS_TOKEN="account1_access_token" \
   X_ACCESS_TOKEN_SECRET="account1_access_token_secret" \
   python app.py subscribe

   # Subscribe Account 2
   X_USER_ID="222222222" \
   X_ACCESS_TOKEN="account2_access_token" \
   X_ACCESS_TOKEN_SECRET="account2_access_token_secret" \
   python app.py subscribe
   ```

   *(Note: You can also run `python app.py import /path/to/account2/like.js` on archives from multiple accounts—`likes.db` deduplicates across all of them seamlessly.)*

---

## 6. Importing Historical Likes (`like.js`)

You can import your official X data archive at any time (even without setting up X API keys or `PUBLIC_WEBHOOK_URL`):

1. Request & download your X Archive (**Settings and privacy → Your account → Download an archive of your data**).
2. Unzip the archive and locate `data/like.js`.
3. Run the importer:

```bash
# Import all historical likes (chronological oldest-first by default)
python app.py import /path/to/twitter-archive/data/like.js

# Test with just 10 likes first and a 2-second delay between posts
python app.py import /path/to/twitter-archive/data/like.js --limit 10 --delay 2.0

# Process newest likes first instead of oldest-first
python app.py import /path/to/twitter-archive/data/like.js --newest-first
```

* **Safe to interrupt & resume:** Every processed (or permanently deleted/404) `tweet_id` is saved in `likes.db` (`DB_PATH`). Pressing `Ctrl+C` and re-running later automatically skips everything already handled.
* **Safe alongside `serve`:** SQLite WAL mode is enabled, so `likes.db` prevents duplicate posts between historical imports and live webhooks.

---

## 7. CLI Reference

| Command | Description |
| :--- | :--- |
| `python app.py serve [--host 0.0.0.0] [--port 8000]` | Starts the FastAPI/Uvicorn server for X webhooks (`/webhook/x`) and health checks (`/healthz`). |
| `python app.py subscribe` | Registers `PUBLIC_WEBHOOK_URL` (`POST /2/webhooks`) and subscribes to `X_USER_ID` outbound `like.create` events (`POST /2/activity/subscriptions`). |
| `python app.py import <path/to/like.js> [--delay 1.5] [--newest-first] [--limit N]` | Parses X archive `like.js` and routes unimported likes through the extraction → classification → Discord pipeline. |

---

## 8. Troubleshooting

* **`subscribe` fails with CRC error / 4xx on `/2/webhooks`:**
  * Confirm the Fly machine is running (`fly status`) and check logs with `fly logs`.
  * Test `curl -i "https://<app-name>.fly.dev/webhook/x?crc_token=ping"`—make sure it returns `200 OK` with JSON, not `502 Bad Gateway`.
  * Double-check `X_CONSUMER_SECRET` matches the exact App whose keys you are using.
* **`401 Invalid X webhook signature` on `POST /webhook/x`:**
  * `X_CONSUMER_SECRET` in `.env` does not match the X Developer App delivering the webhook.
* **Account 2 likes are ignored while Account 1 works:**
  * Check `.env` on your running `serve` process—make sure `X_USER_ID=` is left **blank** (and restart `python app.py serve` after editing `.env`).
* **Posts keep landing in `other`:**
  * Check logs (`LOG_LEVEL=DEBUG` in `.env`) to see whether `gemini-decision-1` returned confidence below `CONFIDENCE_THRESHOLD` or if media downloading failed.
* **Discord returns `404 Unknown Channel (10003)`:**
  * Make sure all 6 `THREAD_ID_*` threads belong to the **exact same parent channel** where `DISCORD_WEBHOOK_URL` was created.