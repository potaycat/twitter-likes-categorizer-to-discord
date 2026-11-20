#!/usr/bin/env python3
"""
app.py — Real-Time & Archive X (Twitter) Likes -> Gemini Decision 1 -> Discord Thread Router

A complete, single-file production service and CLI utility that:
  1. Listens for newly liked posts on X in real time via X Activity API (XAA) push webhooks,
     or imports historical likes from an official X data archive (`like.js`).
  2. Extracts full post metadata and media URLs from BetterTwitFix (`api.vxtwitter.com`).
  3. Classifies text + up to 4 downloaded media images/thumbnails in a single forward pass
     using Google's `gemini-decision-1` model (`v1beta` Decisions API).
  4. Sends rich gallery/video embeds into category-specific threads inside a single parent
     Discord channel via a thread-targeted Discord Webhook, backed by SQLite deduplication.

Dependencies:
    pip install "fastapi>=0.115.0" "uvicorn>=0.32.0" "httpx>=0.27.0" "google-genai>=3.2.0" "python-dotenv>=1.0.0"

CLI Usage:
    python app.py serve [--host 0.0.0.0] [--port 8000]
    python app.py subscribe
    python app.py import /path/to/data/like.js [--delay 1.5] [--newest-first] [--limit N]

Environment Variables:
    Required for classification & Discord delivery (`serve` and `import`):
        GEMINI_API_KEY          Google AI Studio API key for `gemini-decision-1`.
        DISCORD_WEBHOOK_URL     Discord Incoming Webhook URL created on the parent channel.
        THREAD_ID_SFW_FURRY     Discord thread ID for `sfw-furry-art`.
        THREAD_ID_NSFW    Discord thread ID for `nsfw-furry-art`.
        THREAD_ID_MEME          Discord thread ID for `meme`.
        THREAD_ID_FURRY_MEME    Discord thread ID for `furry-meme`.
        THREAD_ID_TECH          Discord thread ID for `tech`.
        THREAD_ID_OTHER         Discord thread ID for `other`.

    Required for X Activity API (`serve` and `subscribe`):
        X_CONSUMER_SECRET       X App Consumer Secret (API Key Secret) used for CRC HMAC-SHA256,
                                webhook signature verification, and OAuth 1.0a signing.
        X_USER_ID               Numeric X account ID whose outbound likes are tracked
                                (required for `subscribe`; can leave blank for `serve`).

    Required additionally for `subscribe`:
        PUBLIC_WEBHOOK_URL      Public HTTPS URL pointing to `/webhook/x`
                                (e.g., `https://example.com/webhook/x`).
        X_CONSUMER_KEY          X App Consumer Key (API Key).
        X_ACCESS_TOKEN          OAuth 1.0a User Access Token for `X_USER_ID` (needs `like.read` / read perms).
        X_ACCESS_TOKEN_SECRET   OAuth 1.0a User Access Token Secret for `X_USER_ID`.

    Optional configuration:
        X_BEARER_TOKEN          App-only OAuth 2.0 Bearer Token (optional fallback for listing
                                or registering webhooks on `GET/POST /2/webhooks`).
        DB_PATH                 SQLite file path for processed tweet deduplication (default: `likes.db`).
        CONFIDENCE_THRESHOLD    Minimum `gemini-decision-1` confidence [0.0–1.0] before falling
                                back to `other` (default: `0.55`).
        GEMINI_IMAGE_RESOLUTION Image token resolution for `gemini-decision-1`:
                                `low`, `medium`, `high`, or `ultra_high` (default: `medium`).
        VXTWITTER_API_BASE      BetterTwitFix API base URL (default: `https://api.vxtwitter.com`).
        IMPORT_DELAY_SECONDS    Delay in seconds between tweets during `import` (default: `1.5`).
        HOST                    HTTP bind address for `serve` (default: `0.0.0.0`).
        PORT                    HTTP bind port for `serve` (default: `8000`).
        LOG_LEVEL               Logging level: DEBUG, INFO, WARNING, ERROR (default: `INFO`).
"""

from __future__ import annotations

import argparse
import ast
import asyncio
import base64
import hashlib
import hmac
import json
import logging
import os
import re
import secrets
import sqlite3
import sys
import threading
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, quote, urlencode, urlparse, urlsplit, urlunsplit

import httpx
import uvicorn
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from google import genai

from dotenv import load_dotenv
load_dotenv()

# ---------------------------------------------------------------------------
# Constants & Category Definitions
# ---------------------------------------------------------------------------

CATEGORIES: dict[str, str] = {
    "sfw-furry-art": (
        "Safe-for-work anthropomorphic/furry artwork, illustrations, fursuits, "
        "or character designs without explicit sexual content."
    ),
    "nsfw": (
        "Explicit, adult, sexually suggestive, or pornographic anthropomorphic/furry "
        "artwork or media."
    ),
    "meme": (
        "General jokes, shitposts, reaction images/videos, humorous screenshots, or internet memes "
        "that are not specifically furry or anthropomorphic."
    ),
    "furry-meme": (
        "Humorous furry or anthropomorphic jokes, shitposts, reaction images/videos, "
        "screenshots, comics, or internet memes."
    ),
    "tech": (
        "Technology news, software, hardware, programming, AI, gadgets, cybersecurity, "
        "science and engineering discussions."
    ),
    "other": (
        "General news, text commentary, non-furry/non-NSFW photography, or anything not "
        "fitting the categories above, including low-confidence classifications."
    ),
}

CATEGORY_COLORS: dict[str, int] = {
    "sfw-furry-art": 0x3BA55D,   # Emerald green
    "nsfw": 0xED4245,            # Crimson red
    "meme": 0xFEE75C,            # Warm yellow
    "furry-meme": 0xEB459E,      # Magenta
    "tech": 0x5865F2,            # Discord blurple
    "other": 0x95A5A6,           # Grey
}

CATEGORY_THREAD_ENV_MAP: dict[str, str] = {
    "sfw-furry-art": "THREAD_ID_SFW_FURRY",
    "nsfw": "THREAD_ID_NSFW",
    "meme": "THREAD_ID_MEME",
    "furry-meme": "THREAD_ID_FURRY_MEME",
    "tech": "THREAD_ID_TECH",
    "other": "THREAD_ID_OTHER",
}

VALID_IMAGE_RESOLUTIONS = {"low", "medium", "high", "ultra_high"}
USER_AGENT = "XLikesDiscordRouter/1.0 (+https://github.com/dylanpdx/BetterTwitFix)"
X_API_BASE = "https://api.x.com"
MAX_GEMINI_IMAGES = 4
MAX_IMAGE_BYTES = 20 * 1024 * 1024  # 20 MiB per image safety cap

logger = logging.getLogger("x_likes_router")


def setup_logging() -> None:
    level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class AppConfig:
    db_path: str
    confidence_threshold: float
    gemini_image_resolution: str
    vxtwitter_api_base: str
    import_delay_seconds: float
    host: str
    port: int

    gemini_api_key: str = ""
    discord_webhook_url: str = ""
    thread_ids: dict[str, str] = field(default_factory=dict)

    x_user_id: str = ""
    x_consumer_key: str = ""
    x_consumer_secret: str = ""
    x_access_token: str = ""
    x_access_token_secret: str = ""
    x_bearer_token: str = ""
    public_webhook_url: str = ""

    @classmethod
    def from_env(cls) -> AppConfig:
        raw_threshold = os.getenv("CONFIDENCE_THRESHOLD", "0.55").strip()
        try:
            threshold = float(raw_threshold)
        except ValueError as exc:
            raise ValueError(f"Invalid CONFIDENCE_THRESHOLD={raw_threshold!r}") from exc
        if not (0.0 <= threshold <= 1.0):
            raise ValueError("CONFIDENCE_THRESHOLD must be between 0.0 and 1.0")

        resolution = os.getenv("GEMINI_IMAGE_RESOLUTION", "medium").strip().lower()
        if resolution not in VALID_IMAGE_RESOLUTIONS:
            raise ValueError(
                f"GEMINI_IMAGE_RESOLUTION must be one of {sorted(VALID_IMAGE_RESOLUTIONS)}, got {resolution!r}"
            )

        raw_delay = os.getenv("IMPORT_DELAY_SECONDS", "1.5").strip()
        try:
            import_delay = max(0.0, float(raw_delay))
        except ValueError as exc:
            raise ValueError(f"Invalid IMPORT_DELAY_SECONDS={raw_delay!r}") from exc

        raw_port = os.getenv("PORT", "8000").strip()
        try:
            port = int(raw_port)
        except ValueError as exc:
            raise ValueError(f"Invalid PORT={raw_port!r}") from exc

        thread_ids = {
            category: os.getenv(env_key, "").strip()
            for category, env_key in CATEGORY_THREAD_ENV_MAP.items()
        }

        return cls(
            db_path=os.getenv("DB_PATH", "likes.db").strip() or "likes.db",
            confidence_threshold=threshold,
            gemini_image_resolution=resolution,
            vxtwitter_api_base=(
                os.getenv("VXTWITTER_API_BASE", "https://api.vxtwitter.com").strip().rstrip("/")
            ),
            import_delay_seconds=import_delay,
            host=os.getenv("HOST", "0.0.0.0").strip() or "0.0.0.0",
            port=port,
            gemini_api_key=os.getenv("GEMINI_API_KEY", "").strip(),
            discord_webhook_url=os.getenv("DISCORD_WEBHOOK_URL", "").strip(),
            thread_ids=thread_ids,
            x_user_id=os.getenv("X_USER_ID", "").strip(),
            x_consumer_key=os.getenv("X_CONSUMER_KEY", "").strip(),
            x_consumer_secret=os.getenv("X_CONSUMER_SECRET", "").strip(),
            x_access_token=os.getenv("X_ACCESS_TOKEN", "").strip(),
            x_access_token_secret=os.getenv("X_ACCESS_TOKEN_SECRET", "").strip(),
            x_bearer_token=os.getenv("X_BEARER_TOKEN", "").strip(),
            public_webhook_url=os.getenv("PUBLIC_WEBHOOK_URL", "").strip(),
        )

    def validate_pipeline(self) -> None:
        missing: list[str] = []
        if not self.gemini_api_key:
            missing.append("GEMINI_API_KEY")
        if not self.discord_webhook_url:
            missing.append("DISCORD_WEBHOOK_URL")
        for category, env_name in CATEGORY_THREAD_ENV_MAP.items():
            if not self.thread_ids.get(category):
                missing.append(env_name)
        if missing:
            raise RuntimeError(
                f"Missing required environment variables for pipeline: {', '.join(missing)}"
            )

    def validate_serve(self) -> None:
        self.validate_pipeline()
        if not self.x_consumer_secret:
            raise RuntimeError("Missing required environment variable for serve: X_CONSUMER_SECRET")
        if not self.x_user_id:
            logger.warning(
                "X_USER_ID is not set; inbound like events on your own posts cannot be filtered by actor ID."
            )

    def validate_subscribe(self) -> None:
        missing: list[str] = []
        for attr, env_name in [
            ("public_webhook_url", "PUBLIC_WEBHOOK_URL"),
            ("x_user_id", "X_USER_ID"),
            ("x_consumer_key", "X_CONSUMER_KEY"),
            ("x_consumer_secret", "X_CONSUMER_SECRET"),
            ("x_access_token", "X_ACCESS_TOKEN"),
            ("x_access_token_secret", "X_ACCESS_TOKEN_SECRET"),
        ]:
            if not getattr(self, attr):
                missing.append(env_name)
        if missing:
            raise RuntimeError(
                f"Missing required environment variables for subscribe: {', '.join(missing)}"
            )
        if not self.public_webhook_url.startswith("https://"):
            raise RuntimeError("PUBLIC_WEBHOOK_URL must be an https:// URL for X webhooks.")


# ---------------------------------------------------------------------------
# SQLite Deduplication Store
# ---------------------------------------------------------------------------


class LikeStore:
    """Thread-safe SQLite store for tracking processed tweet IDs."""

    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self._lock = threading.Lock()
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=10.0)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA busy_timeout=5000;")
        return conn

    def _init_db(self) -> None:
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS processed_likes (
                    tweet_id     TEXT PRIMARY KEY,
                    category     TEXT NOT NULL,
                    confidence   REAL NOT NULL,
                    status       TEXT NOT NULL,
                    tweet_url    TEXT NOT NULL DEFAULT '',
                    processed_at TEXT NOT NULL
                );
                """
            )
            conn.commit()

    def is_processed(self, tweet_id: str) -> bool:
        with self._lock, self._connect() as conn:
            cur = conn.execute(
                "SELECT 1 FROM processed_likes WHERE tweet_id = ? LIMIT 1;",
                (tweet_id,),
            )
            return cur.fetchone() is not None

    def get_all_processed_ids(self) -> set[str]:
        with self._lock, self._connect() as conn:
            cur = conn.execute("SELECT tweet_id FROM processed_likes;")
            return {row[0] for row in cur.fetchall()}

    def mark_processed(
        self,
        tweet_id: str,
        category: str,
        confidence: float,
        status: str = "posted",
        tweet_url: str = "",
    ) -> None:
        now_iso = datetime.now(timezone.utc).isoformat()
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO processed_likes
                    (tweet_id, category, confidence, status, tweet_url, processed_at)
                VALUES (?, ?, ?, ?, ?, ?);
                """,
                (tweet_id, category, confidence, status, tweet_url, now_iso),
            )
            conn.commit()


# ---------------------------------------------------------------------------
# Data Models for Extraction & Classification
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class MediaItem:
    media_type: str  # "image", "video", "gif"
    url: str
    thumbnail_url: str = ""
    alt_text: str = ""

    @property
    def inspectable_image_url(self) -> str:
        """URL yielding still image bytes suitable for `gemini-decision-1` vision input."""
        if self.media_type == "image":
            return self.url
        return self.thumbnail_url or (
            self.url if self.url.lower().endswith((".jpg", ".jpeg", ".png", ".webp")) else ""
        )


@dataclass(frozen=True)
class ExtractedTweet:
    tweet_id: str
    text: str
    user_name: str
    user_screen_name: str
    user_profile_image_url: str
    tweet_url: str
    date_epoch: int | None
    likes: int
    retweets: int
    replies: int
    hashtags: list[str]
    media_urls: list[str]
    media_extended: list[MediaItem]


@dataclass(frozen=True)
class DownloadedImage:
    data_b64: str
    mime_type: str
    source_url: str


@dataclass(frozen=True)
class ClassificationResult:
    category: str
    confidence: float
    raw_choice: str
    fallback_reason: str = ""


class TweetUnavailableError(Exception):
    """Raised when a tweet is permanently unavailable (deleted, suspended, or private)."""


class TweetFetchError(Exception):
    """Raised when BetterTwitFix experiences a transient error."""


# ---------------------------------------------------------------------------
# OAuth 1.0a Signing & X Activity API Helpers
# ---------------------------------------------------------------------------


def _oauth_percent_encode(value: str) -> str:
    return quote(str(value), safe="-._~")


def build_oauth1_authorization_header(
    method: str,
    url: str,
    consumer_key: str,
    consumer_secret: str,
    access_token: str,
    access_token_secret: str,
    form_params: dict[str, str] | None = None,
) -> str:
    """
    Computes an RFC 5849 OAuth 1.0a HMAC-SHA1 Authorization header.
    Note: Per RFC 5849 Section 3.4.1.3.1, `application/json` request bodies are NOT
    included in the OAuth signature base string; only query params and form-encoded params are.
    """
    parsed = urlparse(url)
    scheme = parsed.scheme.lower()
    netloc = parsed.netloc.lower()
    if (scheme == "http" and netloc.endswith(":80")) or (
        scheme == "https" and netloc.endswith(":443")
    ):
        netloc = netloc.rsplit(":", 1)[0]
    base_url = f"{scheme}://{netloc}{parsed.path}"

    oauth_params: dict[str, str] = {
        "oauth_consumer_key": consumer_key,
        "oauth_nonce": secrets.token_hex(16),
        "oauth_signature_method": "HMAC-SHA1",
        "oauth_timestamp": str(int(time.time())),
        "oauth_token": access_token,
        "oauth_version": "1.0",
    }

    sig_params: list[tuple[str, str]] = []
    for k, v in parse_qsl(parsed.query, keep_blank_values=True):
        sig_params.append((_oauth_percent_encode(k), _oauth_percent_encode(v)))
    for k, v in oauth_params.items():
        sig_params.append((_oauth_percent_encode(k), _oauth_percent_encode(v)))
    if form_params:
        for k, v in form_params.items():
            sig_params.append((_oauth_percent_encode(k), _oauth_percent_encode(v)))

    sig_params.sort()
    normalized_params = "&".join(f"{k}={v}" for k, v in sig_params)

    base_string = "&".join(
        [
            _oauth_percent_encode(method.upper()),
            _oauth_percent_encode(base_url),
            _oauth_percent_encode(normalized_params),
        ]
    )
    signing_key = (
        f"{_oauth_percent_encode(consumer_secret)}&{_oauth_percent_encode(access_token_secret)}"
    )
    digest = hmac.new(
        signing_key.encode("ascii"),
        base_string.encode("ascii"),
        hashlib.sha1,
    ).digest()
    oauth_params["oauth_signature"] = base64.b64encode(digest).decode("ascii")

    header_parts = [
        f'{_oauth_percent_encode(k)}="{_oauth_percent_encode(oauth_params[k])}"'
        for k in sorted(oauth_params.keys())
    ]
    return "OAuth " + ", ".join(header_parts)


def compute_x_hmac_sha256_token(secret: str, payload: bytes) -> str:
    digest = hmac.new(secret.encode("utf-8"), payload, hashlib.sha256).digest()
    return "sha256=" + base64.b64encode(digest).decode("ascii")


def verify_x_webhook_signature(secret: str, raw_body: bytes, signature_header: str) -> bool:
    if not signature_header or not secret:
        return False
    expected = compute_x_hmac_sha256_token(secret, raw_body)
    return hmac.compare_digest(expected, signature_header.strip())


def extract_liked_tweet_ids_from_webhook(
    payload: dict[str, Any], target_user_id: str = ""
) -> list[str]:
    """
    Extracts newly liked tweet IDs from X Activity API (XAA) push events,
    while also supporting batched arrays and legacy Account Activity API (AAA) `favorite_events`.
    """
    tweet_ids: list[str] = []
    seen: set[str] = set()

    def _add(candidate: Any) -> None:
        if candidate is None:
            return
        tid = str(candidate).strip()
        if tid.isdigit() and tid not in seen:
            seen.add(tid)
            tweet_ids.append(tid)

    # 1. X Activity API (XAA) envelope: {"data": {"event_type": "like.create", "payload": {...}, ...}}
    events_to_check: list[dict[str, Any]] = []
    if isinstance(payload.get("data"), dict):
        events_to_check.append(payload["data"])
    elif isinstance(payload.get("data"), list):
        events_to_check.extend(item for item in payload["data"] if isinstance(item, dict))
    if isinstance(payload.get("events"), list):
        events_to_check.extend(item for item in payload["events"] if isinstance(item, dict))

    for event in events_to_check:
        event_type = str(event.get("event_type", "")).strip().lower()
        if event_type and event_type != "like.create":
            continue

        inner = event.get("payload") if isinstance(event.get("payload"), dict) else event
        if not isinstance(inner, dict):
            continue

        # Ensure the actor who clicked Like is our tracked X_USER_ID (ignore inbound likes on our tweets)
        actor_id = (
            inner.get("user_id")
            or inner.get("actor_id")
            or inner.get("liker_id")
            or (inner.get("user", {}) if isinstance(inner.get("user"), dict) else {}).get("id")
        )
        if target_user_id and actor_id is not None and str(actor_id).strip() != target_user_id:
            logger.debug(
                "Ignoring like.create event from actor_id=%s (expected X_USER_ID=%s)",
                actor_id,
                target_user_id,
            )
            continue

        # Extract liked post ID across XAA `LikeWithPostAuthor` field variants
        direct_tid = (
            inner.get("liked_tweet_id")
            or inner.get("liked_post_id")
            or inner.get("tweet_id")
            or inner.get("post_id")
            or inner.get("target_tweet_id")
        )
        if direct_tid:
            _add(direct_tid)
            continue

        for nested_key in ("liked_tweet", "liked_post", "tweet", "post", "target"):
            nested = inner.get(nested_key)
            if isinstance(nested, dict) and nested.get("id"):
                _add(nested["id"])
                break
        else:
            # Fallback to includes.tweets / includes.posts if post object was normalized into includes
            includes = event.get("includes") or payload.get("includes")
            if isinstance(includes, dict):
                for inc_key in ("tweets", "posts"):
                    inc_list = includes.get(inc_key)
                    if isinstance(inc_list, list) and len(inc_list) == 1:
                        first_post = inc_list[0]
                        if isinstance(first_post, dict) and first_post.get("id"):
                            _add(first_post["id"])
                            break

    # 2. Legacy Account Activity API (AAA) fallback: `favorite_events`
    fav_events = payload.get("favorite_events")
    if isinstance(fav_events, list):
        for fav in fav_events:
            if not isinstance(fav, dict):
                continue
            liker = fav.get("user") if isinstance(fav.get("user"), dict) else {}
            liker_id = str(liker.get("id_str") or liker.get("id") or "").strip()
            if target_user_id and liker_id and liker_id != target_user_id:
                continue
            status = (
                fav.get("favorited_status")
                if isinstance(fav.get("favorited_status"), dict)
                else {}
            )
            _add(status.get("id_str") or status.get("id"))

    return tweet_ids


# ---------------------------------------------------------------------------
# BetterTwitFix (`api.vxtwitter.com`) Extractor
# ---------------------------------------------------------------------------


def _safe_int(val: Any, default: int = 0) -> int:
    try:
        return int(val)
    except (TypeError, ValueError):
        return default


def _detect_image_mime_type(content: bytes, content_type_header: str, url: str) -> str:
    if content.startswith(b"\xff\xd8\xff"):
        return "image/jpeg"
    if content.startswith(b"\x89PNG\r\n\x1a\n"):
        return "image/png"
    if len(content) >= 12 and content[:4] == b"RIFF" and content[8:12] == b"WEBP":
        return "image/webp"
    if content.startswith((b"GIF87a", b"GIF89a")):
        return "image/gif"

    clean_ct = content_type_header.split(";", 1)[0].strip().lower()
    if clean_ct in {"image/jpeg", "image/png", "image/webp", "image/gif"}:
        return clean_ct
    if clean_ct == "image/jpg":
        return "image/jpeg"

    lower_url = url.lower().split("?", 1)[0]
    if lower_url.endswith(".png"):
        return "image/png"
    if lower_url.endswith(".webp"):
        return "image/webp"
    if lower_url.endswith(".gif"):
        return "image/gif"
    return "image/jpeg"


async def fetch_tweet_from_vxtwitter(
    http_client: httpx.AsyncClient,
    api_base: str,
    tweet_id: str,
    max_attempts: int = 4,
) -> ExtractedTweet:
    url = f"{api_base}/i/status/{tweet_id}"
    last_exc: Exception | None = None

    for attempt in range(1, max_attempts + 1):
        try:
            resp = await http_client.get(
                url,
                headers={"User-Agent": USER_AGENT, "Accept": "application/json"},
                timeout=20.0,
            )
        except httpx.HTTPError as exc:
            last_exc = exc
            wait_s = min(15.0, 1.5 * (2 ** (attempt - 1)))
            logger.warning(
                "vxtwitter network error for tweet_id=%s (attempt %d/%d): %s; retrying in %.1fs",
                tweet_id,
                attempt,
                max_attempts,
                exc,
                wait_s,
            )
            await asyncio.sleep(wait_s)
            continue

        if resp.status_code == 404:
            raise TweetUnavailableError(f"Tweet {tweet_id} returned HTTP 404 from BetterTwitFix.")

        if resp.status_code == 429 or resp.status_code >= 500:
            retry_after_hdr = resp.headers.get("Retry-After", "")
            try:
                wait_s = float(retry_after_hdr)
            except ValueError:
                wait_s = min(20.0, 1.5 * (2 ** (attempt - 1)))
            logger.warning(
                "vxtwitter HTTP %d for tweet_id=%s (attempt %d/%d); retrying in %.1fs",
                resp.status_code,
                tweet_id,
                attempt,
                max_attempts,
                wait_s,
            )
            await asyncio.sleep(wait_s)
            continue

        if resp.status_code >= 400:
            raise TweetUnavailableError(
                f"BetterTwitFix returned HTTP {resp.status_code} for tweet {tweet_id}: {resp.text[:200]}"
            )

        try:
            data = resp.json()
        except ValueError as exc:
            last_exc = exc
            wait_s = min(15.0, 1.5 * (2 ** (attempt - 1)))
            logger.warning(
                "vxtwitter non-JSON response for tweet_id=%s (attempt %d/%d); retrying in %.1fs",
                tweet_id,
                attempt,
                max_attempts,
                wait_s,
            )
            await asyncio.sleep(wait_s)
            continue

        if not isinstance(data, dict):
            raise TweetFetchError(f"Unexpected JSON payload type from BetterTwitFix for {tweet_id}")

        if data.get("error"):
            err_msg = str(data["error"])
            if any(
                token in err_msg.lower()
                for token in ("not found", "suspended", "private", "protected", "unavailable", "nsfw")
            ):
                raise TweetUnavailableError(f"Tweet {tweet_id} unavailable: {err_msg}")
            raise TweetFetchError(f"BetterTwitFix error for {tweet_id}: {err_msg}")

        user_screen_name = str(data.get("user_screen_name") or "").strip().lstrip("@")
        user_name = str(data.get("user_name") or user_screen_name or "Unknown").strip()
        if not user_screen_name and not data.get("text") and not data.get("mediaURLs"):
            raise TweetUnavailableError(f"Tweet {tweet_id} returned empty payload from BetterTwitFix.")

        tweet_url = str(data.get("tweetURL") or "").strip()
        if not tweet_url:
            handle = user_screen_name or "i"
            tweet_url = f"https://x.com/{handle}/status/{tweet_id}"

        raw_epoch = data.get("date_epoch")
        date_epoch = _safe_int(raw_epoch, default=0) if raw_epoch is not None else None
        if date_epoch == 0:
            date_epoch = None

        raw_hashtags = data.get("hashtags")
        hashtags = (
            [str(tag).strip().lstrip("#") for tag in raw_hashtags if str(tag).strip()]
            if isinstance(raw_hashtags, list)
            else []
        )

        raw_media_urls = data.get("mediaURLs")
        media_urls = (
            [str(u).strip() for u in raw_media_urls if str(u).strip()]
            if isinstance(raw_media_urls, list)
            else []
        )

        media_extended: list[MediaItem] = []
        raw_ext = data.get("media_extended")
        if isinstance(raw_ext, list):
            for item in raw_ext:
                if not isinstance(item, dict):
                    continue
                m_url = str(item.get("url") or "").strip()
                if not m_url:
                    continue
                raw_type = str(item.get("type") or "image").strip().lower()
                if raw_type not in {"image", "video", "gif"}:
                    raw_type = "video" if m_url.lower().endswith(".mp4") else "image"
                thumb_url = str(item.get("thumbnail_url") or "").strip()
                alt_text = str(item.get("altText") or "").strip()
                media_extended.append(
                    MediaItem(
                        media_type=raw_type,
                        url=m_url,
                        thumbnail_url=thumb_url,
                        alt_text=alt_text,
                    )
                )

        # Fallback if `mediaURLs` is populated while `media_extended` is omitted
        if not media_extended and media_urls:
            for m_url in media_urls:
                m_type = "video" if m_url.lower().split("?", 1)[0].endswith(".mp4") else "image"
                media_extended.append(MediaItem(media_type=m_type, url=m_url))

        profile_img = str(data.get("user_profile_image_url") or "").strip()

        return ExtractedTweet(
            tweet_id=tweet_id,
            text=str(data.get("text") or ""),
            user_name=user_name,
            user_screen_name=user_screen_name or "unknown",
            user_profile_image_url=profile_img,
            tweet_url=tweet_url,
            date_epoch=date_epoch,
            likes=_safe_int(data.get("likes"), 0),
            retweets=_safe_int(data.get("retweets"), 0),
            replies=_safe_int(data.get("replies"), 0),
            hashtags=hashtags,
            media_urls=media_urls,
            media_extended=media_extended,
        )

    raise TweetFetchError(
        f"Exhausted {max_attempts} attempts fetching tweet {tweet_id} from BetterTwitFix"
    ) from last_exc


async def download_media_images_for_gemini(
    http_client: httpx.AsyncClient,
    tweet: ExtractedTweet,
) -> tuple[list[DownloadedImage], bool]:
    """
    Downloads up to 4 image URLs or video/GIF `thumbnail_url`s as base64-encoded bytes.
    Returns `(downloaded_images, media_failed_flag)`.
    If a tweet contains media items and any target image/thumbnail fails to load (or lacks
    an inspectable image URL), `media_failed_flag` is True so classification safely falls back
    to `other` rather than risking misclassifying unseen NSFW visuals.
    """
    if not tweet.media_extended:
        return [], False

    candidates = tweet.media_extended[:MAX_GEMINI_IMAGES]
    downloaded: list[DownloadedImage] = []

    for idx, item in enumerate(candidates):
        img_url = item.inspectable_image_url
        if not img_url:
            logger.warning(
                "Tweet %s media item #%d (type=%s) has no inspectable image/thumbnail URL.",
                tweet.tweet_id,
                idx + 1,
                item.media_type,
            )
            return downloaded, True

        fetched_bytes: bytes | None = None
        content_type = ""
        for attempt in range(1, 4):
            try:
                resp = await http_client.get(
                    img_url,
                    headers={"User-Agent": USER_AGENT},
                    timeout=20.0,
                )
                if resp.status_code == 200 and resp.content:
                    if len(resp.content) > MAX_IMAGE_BYTES:
                        logger.warning(
                            "Tweet %s media image #%d exceeds %d bytes (%d bytes).",
                            tweet.tweet_id,
                            idx + 1,
                            MAX_IMAGE_BYTES,
                            len(resp.content),
                        )
                        return downloaded, True
                    fetched_bytes = resp.content
                    content_type = resp.headers.get("Content-Type", "")
                    break
                if resp.status_code in {403, 404}:
                    break
            except httpx.HTTPError as exc:
                logger.debug(
                    "Transient error downloading media %s (attempt %d/3): %s",
                    img_url,
                    attempt,
                    exc,
                )
            await asyncio.sleep(0.8 * attempt)

        if not fetched_bytes:
            logger.warning(
                "Failed to download media image #%d (%s) for tweet %s.",
                idx + 1,
                img_url,
                tweet.tweet_id,
            )
            return downloaded, True

        mime_type = _detect_image_mime_type(fetched_bytes, content_type, img_url)
        downloaded.append(
            DownloadedImage(
                data_b64=base64.b64encode(fetched_bytes).decode("ascii"),
                mime_type=mime_type,
                source_url=img_url,
            )
        )

    return downloaded, False


# ---------------------------------------------------------------------------
# Multimodal Classifier (`gemini-decision-1` on `v1beta` Decisions API)
# ---------------------------------------------------------------------------


class TweetClassifier:
    def __init__(
        self,
        api_key: str,
        confidence_threshold: float = 0.55,
        image_resolution: str = "medium",
    ) -> None:
        self.client = genai.Client(
            api_key=api_key,
            http_options={"api_version": "v1beta"},
        )
        self.confidence_threshold = confidence_threshold
        self.image_resolution = image_resolution

    @staticmethod
    def _build_text_block(tweet: ExtractedTweet) -> str:
        hashtags_str = ", ".join(f"#{h}" for h in tweet.hashtags) if tweet.hashtags else "None"
        alt_texts = [
            f"Media #{i + 1} ({m.media_type}): {m.alt_text}"
            for i, m in enumerate(tweet.media_extended[:MAX_GEMINI_IMAGES])
            if m.alt_text
        ]
        alt_block = "\n".join(alt_texts) if alt_texts else "None"
        media_types = (
            ", ".join(m.media_type for m in tweet.media_extended)
            if tweet.media_extended
            else "none (text-only post)"
        )

        return (
            f"X (Twitter) Post Metadata:\n"
            f"- Author: {tweet.user_name} (@{tweet.user_screen_name})\n"
            f"- Hashtags: {hashtags_str}\n"
            f"- Attached Media Types: {media_types}\n"
            f"- Media Alt Text:\n{alt_block}\n\n"
            f"Post Text:\n{tweet.text or '(No text)'}\n"
        )

    async def classify(
        self,
        tweet: ExtractedTweet,
        images: list[DownloadedImage],
        media_failed: bool,
    ) -> ClassificationResult:
        if media_failed:
            return ClassificationResult(
                category="other",
                confidence=0.0,
                raw_choice="other",
                fallback_reason="media_download_failed",
            )

        input_blocks: list[dict[str, Any]] = [
            {"type": "text", "text": self._build_text_block(tweet)}
        ]
        for img in images[:MAX_GEMINI_IMAGES]:
            input_blocks.append(
                {
                    "type": "image",
                    "data": img.data_b64,
                    "mime_type": img.mime_type,
                    "resolution": self.image_resolution,
                }
            )

        questions = {
            "post_category": {
                "type": "choice",
                "instructions": (
                    "Evaluate both the visual media (if attached) and the post metadata/text. "
                    "Select the single label that most accurately classifies this X post. "
                    "Distinguish carefully between anthropomorphic/furry characters vs real humans, "
                    "and between safe-for-work art vs sexually suggestive/explicit adult NSFW content."
                ),
                "criteria": CATEGORIES,
            }
        }

        last_exc: Exception | None = None
        for attempt in range(1, 4):
            try:
                result = await asyncio.to_thread(
                    self.client.decisions.create,
                    model="gemini-decision-1",
                    input=input_blocks,
                    questions=questions,
                )
                answers = getattr(result, "answers", None) or (
                    result.get("answers") if isinstance(result, dict) else None
                )
                if not answers or "post_category" not in answers:
                    raise RuntimeError("Decisions API response missing 'post_category' answer.")

                ans = answers["post_category"]
                raw_choice = str(
                    getattr(ans, "choice", None)
                    or (ans.get("choice") if isinstance(ans, dict) else "")
                ).strip()
                raw_conf = (
                    getattr(ans, "confidence", None)
                    if getattr(ans, "confidence", None) is not None
                    else (ans.get("confidence", 0.0) if isinstance(ans, dict) else 0.0)
                )
                confidence = float(raw_conf)

                if raw_choice not in CATEGORIES:
                    logger.warning(
                        "Unexpected choice %r for tweet %s; falling back to 'other'.",
                        raw_choice,
                        tweet.tweet_id,
                    )
                    return ClassificationResult(
                        category="other",
                        confidence=confidence,
                        raw_choice=raw_choice or "unknown",
                        fallback_reason="invalid_label",
                    )

                if raw_choice != "other" and confidence < self.confidence_threshold:
                    logger.info(
                        "Tweet %s classified as %s with low confidence %.2f (< %.2f); routing to 'other'.",
                        tweet.tweet_id,
                        raw_choice,
                        confidence,
                        self.confidence_threshold,
                    )
                    return ClassificationResult(
                        category="other",
                        confidence=confidence,
                        raw_choice=raw_choice,
                        fallback_reason=f"low_confidence<{self.confidence_threshold:.2f}",
                    )

                return ClassificationResult(
                    category=raw_choice,
                    confidence=confidence,
                    raw_choice=raw_choice,
                )
            except Exception as exc:
                body = getattr(exc, "body", None)
                error = body.get("error") if isinstance(body, dict) else None
                if isinstance(error, dict) and error.get("code") == "prohibited_content":
                    logger.info(
                        "gemini-decision-1 blocked input for tweet %s; routing to 'nsfw'.",
                        tweet.tweet_id,
                    )
                    return ClassificationResult(
                        category="nsfw",
                        confidence=0.0,
                        raw_choice="nsfw",
                        fallback_reason="prohibited_content",
                    )

                last_exc = exc
                wait_s = 1.5 * (2 ** (attempt - 1))
                logger.warning(
                    "gemini-decision-1 error on tweet %s (attempt %d/3): %s; retrying in %.1fs",
                    tweet.tweet_id,
                    attempt,
                    exc,
                    wait_s,
                )
                await asyncio.sleep(wait_s)

        logger.error(
            "gemini-decision-1 failed after 3 attempts for tweet %s (%s); falling back to 'other'.",
            tweet.tweet_id,
            last_exc,
        )
        return ClassificationResult(
            category="other",
            confidence=0.0,
            raw_choice="other",
            fallback_reason="gemini_error",
        )


# ---------------------------------------------------------------------------
# Discord Webhook Thread Router & Embed Builder
# ---------------------------------------------------------------------------

# DESIGN DECISION — Thread Routing & Webhook Architecture:
# All destination threads live inside the same parent Discord channel. Discord's Webhook API
# allows a single channel-level webhook (`DISCORD_WEBHOOK_URL`) to post into any thread in that
# parent channel simply by appending `?thread_id=<THREAD_ID>`, and Discord automatically unarchives
# archived threads when a webhook message arrives. Using a single webhook URL parameterized per
# post avoids managing separate webhook secrets while enabling clean fallback to `THREAD_ID_OTHER`
# if a category thread is ever deleted (HTTP 404 / Discord code 10003).
#
# HOW TO OBTAIN DISCORD THREAD IDs:
#   1. In Discord, open User Settings -> Advanced -> enable "Developer Mode".
#   2. Right-click each target thread in the channel list (or click the thread header menu)
#      and click "Copy Thread ID".
#   3. Set THREAD_ID_SFW_FURRY, THREAD_ID_NSFW, THREAD_ID_MEME, THREAD_ID_FURRY_MEME,
#      THREAD_ID_TECH, and THREAD_ID_OTHER in your environment or `.env` file.
#
# DESIGN DECISION — Video & GIF Playback in Discord:
# Discord ignores `embeds[].video` on incoming webhook payloads, and `embeds[].image` only renders
# static images. Posting a `vxtwitter.com` link in `content` would cause Discord to unfurl a second,
# duplicate text/author embed alongside our custom rich embed. Instead, when `media_extended`
# contains `"video"` or `"gif"` streams, we place the direct `https://video.twimg.com/...mp4` stream
# URL(s) into `content` while keeping our custom rich embed (with the video `thumbnail_url`, tweet
# text, stats, and category badge) in `embeds`. Discord's client renders direct `.mp4` links as a
# clean native inline video player right above our rich metadata embed without duplicate text cards.


def _build_thread_webhook_url(base_webhook_url: str, thread_id: str) -> str:
    parts = urlsplit(base_webhook_url)
    query_dict = dict(parse_qsl(parts.query, keep_blank_values=True))
    query_dict["thread_id"] = thread_id
    return urlunsplit(
        (parts.scheme, parts.netloc, parts.path, urlencode(query_dict), parts.fragment)
    )


def _truncate(text: str, limit: int) -> str:
    if len(text) <= limit:
        return text
    return text[: limit - 1].rstrip() + "…"


def build_discord_webhook_payload(
    tweet: ExtractedTweet,
    classification: ClassificationResult,
) -> dict[str, Any]:
    author_label = _truncate(f"{tweet.user_name} (@{tweet.user_screen_name})", 256)
    author_obj: dict[str, Any] = {
        "name": author_label,
        "url": f"https://x.com/{tweet.user_screen_name}",
    }
    if tweet.user_profile_image_url.startswith(("https://", "http://")):
        author_obj["icon_url"] = tweet.user_profile_image_url

    badge_text = f"`{classification.category}` ({classification.confidence:.0%} • classifier-261120)"
    if classification.fallback_reason:
        badge_text += f" *({classification.fallback_reason})*"

    stats_parts = [f"❤️ {tweet.likes:,}", f"🔁 {tweet.retweets:,}"]
    if tweet.replies:
        stats_parts.append(f"💬 {tweet.replies:,}")
    stats_value = "  ".join(stats_parts)

    fields: list[dict[str, Any]] = [
        {"name": "Category", "value": badge_text, "inline": True},
        {"name": "Engagement", "value": stats_value, "inline": True},
    ]
    if tweet.hashtags:
        tags_display = _truncate(" ".join(f"`#{t}`" for t in tweet.hashtags[:12]), 1024)
        fields.append({"name": "Hashtags", "value": tags_display, "inline": False})

    primary_embed: dict[str, Any] = {
        "author": author_obj,
        "title": f"Post by @{tweet.user_screen_name}",
        "url": tweet.tweet_url,
        "color": CATEGORY_COLORS.get(classification.category, CATEGORY_COLORS["other"]),
        "fields": fields,
        "footer": {"text": f"Tweet ID: {tweet.tweet_id}"},
    }

    if tweet.text.strip():
        # Discord enforces 4096 chars per description (and 6000 chars across all embeds in a message)
        primary_embed["description"] = _truncate(tweet.text.strip(), 4096)

    if tweet.date_epoch is not None:
        primary_embed["timestamp"] = datetime.fromtimestamp(
            tweet.date_epoch, tz=timezone.utc
        ).isoformat()

    image_items = [m for m in tweet.media_extended if m.media_type == "image" and m.url]
    video_items = [
        m for m in tweet.media_extended if m.media_type in {"video", "gif"} and m.url
    ]

    embeds: list[dict[str, Any]] = [primary_embed]

    # Set primary embed image (first photo, or first video's thumbnail)
    if image_items:
        first_img = image_items[0]
        primary_embed["image"] = {"url": first_img.url}
        # Attach up to 3 additional image embeds sharing the exact same `url` (`tweet.tweet_url`)
        # so Discord merges them into a unified 2–4 image gallery card.
        for extra_img in image_items[1:4]:
            embeds.append(
                {
                    "url": tweet.tweet_url,
                    "image": {"url": extra_img.url},
                }
            )
    elif video_items and video_items[0].thumbnail_url:
        primary_embed["image"] = {"url": video_items[0].thumbnail_url}

    payload: dict[str, Any] = {
        "embeds": embeds,
        "allowed_mentions": {"parse": []},
    }

    if video_items:
        video_links = [m.url for m in video_items[:4] if m.url.startswith(("https://", "http://"))]
        if video_links:
            payload["content"] = _truncate("\n".join(video_links), 2000)

    return payload


async def send_discord_tweet_link(
    http_client: httpx.AsyncClient,
    webhook_url: str,
    tweet_url: str,
) -> None:
    resp = await http_client.post(
        webhook_url,
        json={"content": tweet_url},
        timeout=20.0,
    )
    if resp.status_code not in {200, 204}:
        raise RuntimeError(
            f"Discord fallback failed with HTTP {resp.status_code}: {resp.text[:300]}"
        )


async def send_discord_embed_to_thread(
    http_client: httpx.AsyncClient,
    webhook_url: str,
    thread_ids: dict[str, str],
    tweet: ExtractedTweet,
    classification: ClassificationResult,
    max_attempts: int = 5,
) -> None:
    target_thread_id = thread_ids.get(classification.category) or thread_ids["other"]
    fallback_thread_id = thread_ids["other"]
    payload = build_discord_webhook_payload(tweet, classification)

    current_thread_id = target_thread_id
    for attempt in range(1, max_attempts + 1):
        target_url = _build_thread_webhook_url(webhook_url, current_thread_id)
        try:
            resp = await http_client.post(target_url, json=payload, timeout=20.0)
        except httpx.HTTPError as exc:
            wait_s = min(20.0, 1.5 * (2 ** (attempt - 1)))
            logger.warning(
                "Discord network error for tweet %s (attempt %d/%d): %s; retrying in %.1fs",
                tweet.tweet_id,
                attempt,
                max_attempts,
                exc,
                wait_s,
            )
            await asyncio.sleep(wait_s)
            continue

        if resp.status_code in {200, 204}:
            remaining = resp.headers.get("X-RateLimit-Remaining")
            reset_after = resp.headers.get("X-RateLimit-Reset-After")
            if remaining == "0" and reset_after:
                try:
                    pause_s = min(10.0, max(0.1, float(reset_after)))
                    await asyncio.sleep(pause_s)
                except ValueError:
                    pass
            return

        if resp.status_code == 429:
            retry_after_s = 2.0
            header_val = resp.headers.get("Retry-After")
            if header_val:
                try:
                    retry_after_s = float(header_val)
                except ValueError:
                    pass
            else:
                try:
                    body_json = resp.json()
                    retry_after_s = float(body_json.get("retry_after", 2.0))
                except (ValueError, TypeError):
                    pass
            sleep_for = max(0.25, retry_after_s) + 0.15
            logger.warning(
                "Discord 429 rate limit hit for tweet %s; sleeping %.2fs (attempt %d/%d)",
                tweet.tweet_id,
                sleep_for,
                attempt,
                max_attempts,
            )
            await asyncio.sleep(sleep_for)
            continue

        # If target category thread was deleted/missing, fall back once to `THREAD_ID_OTHER`
        if resp.status_code == 404 and current_thread_id != fallback_thread_id:
            try:
                err_code = resp.json().get("code")
            except ValueError:
                err_code = None
            if err_code == 10003:  # Unknown Channel / Thread
                logger.error(
                    "Discord thread %s not found (code 10003); falling back to THREAD_ID_OTHER (%s).",
                    current_thread_id,
                    fallback_thread_id,
                )
                current_thread_id = fallback_thread_id
                continue

        if resp.status_code >= 500:
            wait_s = min(20.0, 1.5 * (2 ** (attempt - 1)))
            logger.warning(
                "Discord HTTP %d for tweet %s (attempt %d/%d); retrying in %.1fs",
                resp.status_code,
                tweet.tweet_id,
                attempt,
                max_attempts,
                wait_s,
            )
            await asyncio.sleep(wait_s)
            continue

        raise RuntimeError(
            f"Discord webhook failed with HTTP {resp.status_code} for tweet {tweet.tweet_id}: {resp.text[:300]}"
        )

    raise RuntimeError(
        f"Exhausted {max_attempts} Discord webhook delivery attempts for tweet {tweet.tweet_id}."
    )


# ---------------------------------------------------------------------------
# End-to-End Like Processing Pipeline
# ---------------------------------------------------------------------------


class LikeRouterPipeline:
    def __init__(self, config: AppConfig, store: LikeStore) -> None:
        self.config = config
        self.store = store
        self.classifier = TweetClassifier(
            api_key=config.gemini_api_key,
            confidence_threshold=config.confidence_threshold,
            image_resolution=config.gemini_image_resolution,
        )
        self.http_client = httpx.AsyncClient(follow_redirects=True)
        self._inflight: set[str] = set()
        self._inflight_lock = asyncio.Lock()

    async def aclose(self) -> None:
        await self.http_client.aclose()

    async def process_tweet_id(self, tweet_id: str, check_duplicate: bool = False) -> str:
        """
        Runs deduplication -> BetterTwitFix extraction -> Gemini Decision 1 classification ->
        Discord thread webhook post -> SQLite persistence.
        Returns one of: `'posted'`, `'duplicate'`, `'unavailable'`, or `'error'`.
        """
        clean_id = tweet_id.strip()
        if not clean_id.isdigit():
            logger.warning("Skipping invalid non-numeric tweet_id=%r", tweet_id)
            return "error"

        async with self._inflight_lock:
            if clean_id in self._inflight or (check_duplicate and self.store.is_processed(clean_id)):
                logger.debug("Skipping already processed/in-flight tweet_id=%s", clean_id)
                return "duplicate"
            self._inflight.add(clean_id)

        try:
            try:
                tweet = await fetch_tweet_from_vxtwitter(
                    self.http_client,
                    self.config.vxtwitter_api_base,
                    clean_id,
                )
            # except TweetUnavailableError as exc:
            #     logger.warning("Tweet %s unavailable (%s); recording to skip in future.", clean_id, exc)
            #     self.store.mark_processed(
            #         tweet_id=clean_id,
            #         category="unavailable",
            #         confidence=0.0,
            #         status="unavailable",
            #         tweet_url=f"https://x.com/i/status/{clean_id}",
            #     )
            #     return "unavailable"
            except Exception as exc:
                tweet_url = f"https://x.com/i/status/{clean_id}"
                logger.warning(
                    "Tweet %s could not be fetched; sending link to Discord (%s)",
                    clean_id,
                    exc,
                )
                await send_discord_tweet_link(
                    http_client=self.http_client,
                    webhook_url=self.config.discord_webhook_url,
                    tweet_url=tweet_url,
                )
                return "posted"

            images, media_failed = await download_media_images_for_gemini(
                self.http_client, tweet
            )
            classification = await self.classifier.classify(tweet, images, media_failed)

            await send_discord_embed_to_thread(
                http_client=self.http_client,
                webhook_url=self.config.discord_webhook_url,
                thread_ids=self.config.thread_ids,
                tweet=tweet,
                classification=classification,
            )

            self.store.mark_processed(
                tweet_id=clean_id,
                category=classification.category,
                confidence=classification.confidence,
                status="posted",
                tweet_url=tweet.tweet_url,
            )
            logger.info(
                "Routed tweet_id=%s (@%s) -> %s (confidence=%.0f%%)",
                clean_id,
                tweet.user_screen_name,
                classification.category,
                classification.confidence * 100.0,
            )
            return "posted"
        except Exception as exc:
            logger.error("Failed processing tweet_id=%s: %s", clean_id, exc, exc_info=True)
            return "error"
        finally:
            async with self._inflight_lock:
                self._inflight.discard(clean_id)


# ---------------------------------------------------------------------------
# FastAPI Webhook Server (`python app.py serve`)
# ---------------------------------------------------------------------------


def create_fastapi_app(config: AppConfig) -> FastAPI:
    store = LikeStore(config.db_path)
    pipeline = LikeRouterPipeline(config, store)
    queue: asyncio.Queue[str] = asyncio.Queue()

    async def _worker() -> None:
        logger.info("Background X like worker started (db=%s).", config.db_path)
        while True:
            tweet_id = await queue.get()
            try:
                await pipeline.process_tweet_id(tweet_id)
            finally:
                queue.task_done()

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        worker_task = asyncio.create_task(_worker())
        try:
            yield
        finally:
            worker_task.cancel()
            try:
                await worker_task
            except asyncio.CancelledError:
                pass
            await pipeline.aclose()

    app = FastAPI(
        title="X Likes -> Gemini Decision 1 -> Discord Thread Router",
        version="1.0.0",
        lifespan=lifespan,
    )

    @app.get("/healthz")
    async def healthz() -> dict[str, Any]:
        return {"status": "ok", "queued_likes": queue.qsize()}

    @app.get("/webhook/x")
    async def x_webhook_crc(crc_token: str = Query(..., min_length=1)) -> JSONResponse:
        """Handles X's Challenge-Response Check (CRC) via HMAC-SHA256."""
        response_token = compute_x_hmac_sha256_token(
            config.x_consumer_secret,
            crc_token.encode("utf-8"),
        )
        logger.info("Answered X webhook CRC challenge.")
        return JSONResponse(status_code=200, content={"response_token": response_token})

    @app.post("/webhook/x")
    async def x_webhook_receive(request: Request) -> JSONResponse:
        """Verifies X's HMAC-SHA256 signature and queues newly liked tweet IDs."""
        raw_body = await request.body()
        sig_header = (
            request.headers.get("x-twitter-webhooks-signature")
            or request.headers.get("x-twitter-webhooks-signature-oauth2")
            or ""
        )
        if not verify_x_webhook_signature(config.x_consumer_secret, raw_body, sig_header):
            logger.warning("Rejected POST /webhook/x due to invalid or missing webhook signature.")
            raise HTTPException(status_code=401, detail="Invalid X webhook signature.")

        try:
            payload = json.loads(raw_body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise HTTPException(status_code=400, detail="Malformed JSON body.") from exc

        if not isinstance(payload, dict):
            return JSONResponse(status_code=200, content={"status": "ignored"})

        liked_ids = extract_liked_tweet_ids_from_webhook(payload, config.x_user_id)
        enqueued = 0
        for tid in liked_ids:
            queue.put_nowait(tid)
            enqueued += 1

        if liked_ids:
            logger.info(
                "Received X webhook: %d like event(s), %d newly enqueued.",
                len(liked_ids),
                enqueued,
            )
        return JSONResponse(status_code=200, content={"status": "ok", "enqueued": enqueued})

    return app


# ---------------------------------------------------------------------------
# CLI Mode: `subscribe` (Register X v2 Webhook + Create XAA Like Subscription)
# ---------------------------------------------------------------------------


async def run_subscribe(config: AppConfig) -> None:
    config.validate_subscribe()

    def _user_oauth_headers(method: str, url: str) -> dict[str, str]:
        return {
            "Authorization": build_oauth1_authorization_header(
                method=method,
                url=url,
                consumer_key=config.x_consumer_key,
                consumer_secret=config.x_consumer_secret,
                access_token=config.x_access_token,
                access_token_secret=config.x_access_token_secret,
            ),
            "User-Agent": USER_AGENT,
        }

    def _webhook_mgmt_headers(method: str, url: str) -> dict[str, str]:
        # X v2 Webhooks management (`GET/POST/PUT /2/webhooks`) accepts App Bearer Token
        # or OAuth 1.0a credentials; prefer X_BEARER_TOKEN when configured, else OAuth 1.0a.
        if config.x_bearer_token:
            return {
                "Authorization": f"Bearer {config.x_bearer_token}",
                "User-Agent": USER_AGENT,
            }
        return _user_oauth_headers(method, url)

    async with httpx.AsyncClient(timeout=30.0) as client:
        webhooks_url = f"{X_API_BASE}/2/webhooks"
        logger.info("Checking existing X webhooks via GET %s ...", webhooks_url)
        list_resp = await client.get(
            webhooks_url,
            headers=_webhook_mgmt_headers("GET", webhooks_url),
        )
        if list_resp.status_code >= 400 and config.x_bearer_token:
            logger.info("Retrying GET /2/webhooks with OAuth 1.0a User Context...")
            list_resp = await client.get(
                webhooks_url,
                headers=_user_oauth_headers("GET", webhooks_url),
            )

        webhook_id = ""
        if list_resp.status_code == 200:
            list_data = list_resp.json().get("data", [])
            if isinstance(list_data, list):
                for wh in list_data:
                    if isinstance(wh, dict) and wh.get("url") == config.public_webhook_url:
                        webhook_id = str(wh.get("id", "")).strip()
                        is_valid = wh.get("valid", True)
                        logger.info(
                            "Found existing webhook id=%s for %s (valid=%s).",
                            webhook_id,
                            config.public_webhook_url,
                            is_valid,
                        )
                        if webhook_id and not is_valid:
                            crc_url = f"{X_API_BASE}/2/webhooks/{webhook_id}"
                            logger.info("Triggering CRC re-validation via PUT %s ...", crc_url)
                            crc_resp = await client.put(
                                crc_url,
                                headers=_webhook_mgmt_headers("PUT", crc_url),
                            )
                            if crc_resp.status_code >= 400:
                                raise RuntimeError(
                                    f"Webhook CRC re-validation failed ({crc_resp.status_code}): {crc_resp.text}"
                                )
                        break
        else:
            logger.warning(
                "GET /2/webhooks returned HTTP %d (%s); proceeding to register webhook.",
                list_resp.status_code,
                list_resp.text[:250],
            )

        if not webhook_id:
            logger.info(
                "Registering PUBLIC_WEBHOOK_URL=%s via POST %s ...",
                config.public_webhook_url,
                webhooks_url,
            )
            reg_headers = {
                **_webhook_mgmt_headers("POST", webhooks_url),
                "Content-Type": "application/json",
            }
            reg_resp = await client.post(
                webhooks_url,
                headers=reg_headers,
                json={"url": config.public_webhook_url},
            )
            if reg_resp.status_code >= 400 and config.x_bearer_token:
                reg_resp = await client.post(
                    webhooks_url,
                    headers={
                        **_user_oauth_headers("POST", webhooks_url),
                        "Content-Type": "application/json",
                    },
                    json={"url": config.public_webhook_url},
                )
            if reg_resp.status_code >= 400:
                raise RuntimeError(
                    f"Failed to register webhook at POST /2/webhooks (HTTP {reg_resp.status_code}): {reg_resp.text}"
                )
            reg_json = reg_resp.json()
            webhook_id = str(
                (reg_json.get("data") or {}).get("id") or reg_json.get("id") or ""
            ).strip()
            if not webhook_id:
                raise RuntimeError(
                    f"Webhook registration succeeded without returning a webhook id: {reg_json}"
                )
            logger.info("Registered X webhook successfully: webhook_id=%s", webhook_id)

        # Create X Activity API (XAA) subscription: POST /2/activity/subscriptions
        subs_url = f"{X_API_BASE}/2/activity/subscriptions"
        logger.info("Checking existing X Activity API subscriptions via GET %s ...", subs_url)
        existing_subs_resp = await client.get(
            subs_url,
            headers=_user_oauth_headers("GET", subs_url),
        )
        if existing_subs_resp.status_code == 200:
            subs_list = existing_subs_resp.json().get("data", [])
            if isinstance(subs_list, list):
                for sub in subs_list:
                    if not isinstance(sub, dict):
                        continue
                    sub_filter = sub.get("filter") if isinstance(sub.get("filter"), dict) else {}
                    if (
                        sub.get("event_type") == "like.create"
                        and str(sub_filter.get("user_id", "")) == config.x_user_id
                        and str(sub.get("webhook_id", webhook_id)) == webhook_id
                    ):
                        logger.info(
                            "Active XAA like.create subscription already exists (subscription_id=%s).",
                            sub.get("id") or sub.get("subscription_id"),
                        )
                        return

        subscription_body: dict[str, Any] = {
            "event_type": "like.create",
            "filter": {
                "user_id": config.x_user_id,
                "direction": "outbound",
            },
            "webhook_id": webhook_id,
            "tag": "x-likes-discord-router",
        }
        logger.info(
            "Creating X Activity API subscription for user_id=%s on webhook_id=%s via POST %s ...",
            config.x_user_id,
            webhook_id,
            subs_url,
        )
        sub_resp = await client.post(
            subs_url,
            headers={
                **_user_oauth_headers("POST", subs_url),
                "Content-Type": "application/json",
            },
            json=subscription_body,
        )
        if sub_resp.status_code == 409:
            logger.info("XAA subscription already exists (HTTP 409): %s", sub_resp.text)
            return
        if sub_resp.status_code >= 400:
            raise RuntimeError(
                f"Failed creating XAA subscription at POST /2/activity/subscriptions "
                f"(HTTP {sub_resp.status_code}): {sub_resp.text}"
            )

        logger.info("Created X Activity API subscription successfully: %s", sub_resp.json())


# ---------------------------------------------------------------------------
# CLI Mode: `import` (Parse X Data Archive `like.js` & Route Historical Likes)
# ---------------------------------------------------------------------------


def parse_x_archive_like_js(file_path: Path) -> list[str]:
    """
    Parses X's official archive `data/like.js` by stripping `window.YTD.like.part0 = `
    (as well as multi-part prefixes like `window.YTD.like.part1 = ` and trailing semicolons)
    and extracting ordered `like.tweetId` strings.
    """
    if not file_path.is_file():
        raise FileNotFoundError(f"Archive file not found: {file_path}")

    raw_text = file_path.read_text(encoding="utf-8-sig").strip()
    if raw_text.startswith("window.YTD.like.part0 = "):
        json_str = raw_text[len("window.YTD.like.part0 = ") :].strip()
    else:
        json_str = re.sub(
            r"^\s*window\.YTD\.like\.part\d+\s*=\s*",
            "",
            raw_text,
            count=1,
        ).strip()

    if json_str.endswith(";"):
        json_str = json_str[:-1].strip()

    try:
        entries = json.loads(json_str)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Failed to parse JSON array from {file_path}: {exc}") from exc

    if not isinstance(entries, list):
        raise ValueError(f"Expected top-level JSON array in {file_path}, got {type(entries).__name__}")

    tweet_ids: list[str] = []
    seen: set[str] = set()
    for item in entries:
        if not isinstance(item, dict):
            continue
        like_obj = item.get("like") if isinstance(item.get("like"), dict) else item
        tid = str(like_obj.get("tweetId") or like_obj.get("id_str") or "").strip()
        if tid.isdigit() and tid not in seen:
            seen.add(tid)
            tweet_ids.append(tid)

    return tweet_ids


async def run_import(
    config: AppConfig,
    like_js_path: Path,
    delay_seconds: float,
    newest_first: bool = False,
    limit: int | None = None,
) -> None:
    config.validate_pipeline()
    all_ids = parse_x_archive_like_js(like_js_path)
    if not newest_first:
        # X archives list newest likes first; reversing posts chronological history into Discord threads
        all_ids.reverse()

    store = LikeStore(config.db_path)
    already_done = store.get_all_processed_ids()
    pending_ids = [tid for tid in all_ids if tid not in already_done]
    skipped_existing = len(all_ids) - len(pending_ids)

    if limit is not None and limit >= 0:
        pending_ids = pending_ids[:limit]

    logger.info(
        "Parsed %d unique likes from %s (%d already in SQLite, %d queued to process).",
        len(all_ids),
        like_js_path,
        skipped_existing,
        len(pending_ids),
    )
    if not pending_ids:
        logger.info("Nothing new to import. Exiting cleanly.")
        return

    pipeline = LikeRouterPipeline(config, store)
    counts = {"posted": 0, "unavailable": 0, "duplicate": 0, "error": 0}
    total = len(pending_ids)

    try:
        for index, tid in enumerate(pending_ids, start=1):
            logger.info("[%d/%d] Importing liked tweet_id=%s ...", index, total, tid)
            outcome = await pipeline.process_tweet_id(tid, check_duplicate=True)
            counts[outcome] = counts.get(outcome, 0) + 1
            if index < total and delay_seconds > 0:
                await asyncio.sleep(delay_seconds)
    finally:
        await pipeline.aclose()

    logger.info(
        "Import complete: posted=%d, unavailable=%d, duplicate=%d, errors=%d.",
        counts["posted"],
        counts["unavailable"],
        counts["duplicate"],
        counts["error"],
    )


# ---------------------------------------------------------------------------
# CLI Entrypoint
# ---------------------------------------------------------------------------


def build_cli_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="app.py",
        description="Real-time & archive X Likes -> Gemini Decision 1 -> Discord Thread Router",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    serve_parser = subparsers.add_parser(
        "serve",
        help="Start FastAPI server listening for X Activity API webhooks on /webhook/x",
    )
    serve_parser.add_argument("--host", default=None, help="Override bind host (default: $HOST or 0.0.0.0)")
    serve_parser.add_argument("--port", type=int, default=None, help="Override bind port (default: $PORT or 8000)")

    subparsers.add_parser(
        "subscribe",
        help="Register PUBLIC_WEBHOOK_URL with X and subscribe to X_USER_ID like.create events",
    )

    import_parser = subparsers.add_parser(
        "import",
        help="Import historical likes from an official X archive like.js file",
    )
    import_parser.add_argument("like_js", type=Path, help="Path to X archive data/like.js")
    import_parser.add_argument(
        "--delay",
        type=float,
        default=None,
        help="Delay in seconds between processed likes (default: $IMPORT_DELAY_SECONDS or 1.5)",
    )
    import_parser.add_argument(
        "--newest-first",
        action="store_true",
        help="Process archive in original newest-first order instead of chronological oldest-first",
    )
    import_parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional cap on how many unimported likes to process in this run",
    )

    return parser


def main() -> None:
    setup_logging()
    parser = build_cli_parser()
    args = parser.parse_args()

    try:
        config = AppConfig.from_env()
        if args.command == "serve":
            config.validate_serve()
            bind_host = args.host or config.host
            bind_port = args.port or config.port
            fastapi_app = create_fastapi_app(config)
            uvicorn.run(fastapi_app, host=bind_host, port=bind_port, log_level="info")
        elif args.command == "subscribe":
            asyncio.run(run_subscribe(config))
        elif args.command == "import":
            delay = args.delay if args.delay is not None else config.import_delay_seconds
            asyncio.run(
                run_import(
                    config=config,
                    like_js_path=args.like_js,
                    delay_seconds=max(0.0, delay),
                    newest_first=args.newest_first,
                    limit=args.limit,
                )
            )
    except KeyboardInterrupt:
        logger.info("Interrupted by user; shutting down.")
        sys.exit(130)
    except Exception as exc:
        logger.error("%s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()