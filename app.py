#!/usr/bin/env python3
"""
AudioBookshelf Chromecast Bridge
Streams audiobooks from AudioBookshelf to Chromecast devices,
monitoring playback progress and syncing back to ABS.
"""

import asyncio
import base64
import difflib
import json
import logging
import os
import sqlite3
import threading
import time
from dataclasses import dataclass, field, asdict
from typing import Optional
import uuid

import pychromecast
import requests
import zeroconf
from flask import Flask, jsonify, request, Response
from flask_cors import CORS
from pychromecast.controllers.media import MediaController

import db as database

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

app = Flask(__name__, static_folder="static", static_url_path="/static")
CORS(app)

# ---------------------------------------------------------------------------
# Config (env vars or defaults)
# ---------------------------------------------------------------------------
ABS_URL        = os.getenv("ABS_URL", "http://localhost:13378")
# ABS_PUBLIC_URL is used for Chromecast-facing stream/cover URLs.
# Set this to your public HTTPS URL (e.g. https://abs.ablank.life) so
# Chromecasts can reach the stream. Falls back to ABS_URL if not set.
ABS_PUBLIC_URL = os.getenv("ABS_PUBLIC_URL", "").rstrip("/") or ABS_URL
# Tag used to filter the kids/series view. Books must have this tag to appear.
# Leave empty to show all books. e.g. "For Flora"
ABS_KIDS_TAG   = os.getenv("ABS_KIDS_TAG", "").strip()
ABS_TOKEN = os.getenv("ABS_TOKEN", "")
PROGRESS_INTERVAL = int(os.getenv("PROGRESS_INTERVAL", "15"))   # seconds
HOST_IP = os.getenv("HOST_IP", "0.0.0.0")
PORT = int(os.getenv("PORT", "8123"))
DEFAULT_DEVICE = os.getenv("DEFAULT_DEVICE", "")   # friendly name of preferred Chromecast
SERIES_RESTART_TAIL_PCT = float(os.getenv("SERIES_RESTART_TAIL_PCT", "0.05"))  # 0.0–1.0; default 5%
CACHE_REFRESH_INTERVAL  = int(os.getenv("CACHE_REFRESH_INTERVAL", "60"))       # seconds between background refresh checks

# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------
cast_sessions: dict[str, "CastSession"] = {}
discovered_devices: list[dict] = []
discovery_lock = threading.Lock()

# ---------------------------------------------------------------------------
# Background cache refresh
# ---------------------------------------------------------------------------

def _book_has_kids_tag(book: dict) -> bool:
    """Return True if ABS_KIDS_TAG is unset, or if the book has the tag."""
    if not ABS_KIDS_TAG:
        return True
    tags = book.get("media", {}).get("tags") or []
    return ABS_KIDS_TAG.lower() in [t.lower() for t in tags]


def _refresh_libraries_and_series(force: bool = False):
    """Fetch libraries + series from ABS and store in DB cache."""
    try:
        if force or database.libraries_stale():
            libs = get_libraries()
            database.upsert_libraries(libs)
        else:
            libs = database.get_libraries_cached()

        for lib in libs:
            if force or database.series_stale(lib["id"]):
                try:
                    raw_series = abs_get(f"/libraries/{lib['id']}/series?limit=500&sort=name").get("results", [])
                    # Resolve default device once per library refresh
                    cached_devices = database.get_devices_cached()
                    default_device = next((d["name"] for d in cached_devices if d.get("is_default")), DEFAULT_DEVICE or "")
                    enriched = []
                    for s in raw_series:
                        name = s.get("name") or s.get("nameIgnorePrefix") or s["id"]
                        params = f"library={lib['id']}&series={s['id']}"
                        if default_device:
                            params += f"&device={default_device}"
                        cover_item_id = None
                        book_count = 0
                        try:
                            books = get_series_books(lib["id"], s["id"])
                            tagged = [b for b in books if _book_has_kids_tag(b)]
                            book_count = len(tagged)
                            if book_count == 0:
                                continue  # skip series with no tagged books
                            cover_item_id = tagged[0]["id"]
                        except Exception:
                            pass
                        enriched.append({
                            "series_id": s["id"],
                            "series_name": name,
                            "book_count": book_count,
                            "cover_item_id": cover_item_id,
                            "cover_url": f"{ABS_PUBLIC_URL}/api/items/{cover_item_id}/cover?token={ABS_TOKEN}" if cover_item_id else None,
                            "qr_url": f"/play?{params}",
                        })
                    if enriched:  # only upsert if we got results — don't wipe on empty response
                        database.upsert_series(lib["id"], enriched)
                except Exception as e:
                    log.warning(f"Series refresh failed for library {lib['id']}: {e}")
    except Exception as e:
        log.warning(f"Library/series refresh failed: {e}")


def _refresh_devices(force: bool = False):
    """Discover Chromecast devices and cache them."""
    try:
        if force or database.devices_stale():
            devices = discover_devices()
            if DEFAULT_DEVICE:
                match = fuzzy_match_device(DEFAULT_DEVICE, devices)
                for d in devices:
                    d["is_default"] = (match is not None and d["name"] == match["name"])
            database.upsert_devices(devices)
    except Exception as e:
        log.warning(f"Device refresh failed: {e}")


def _background_refresh():
    """Background thread: keep library, series, and device caches fresh."""
    # Stagger the first runs to avoid hammering on startup
    time.sleep(3)
    log.info("Background refresh thread started")
    while True:
        _refresh_libraries_and_series()
        _refresh_devices()
        time.sleep(CACHE_REFRESH_INTERVAL)

# ---------------------------------------------------------------------------
# ABS API helpers
# ---------------------------------------------------------------------------

def abs_get(path: str) -> dict:
    r = requests.get(f"{ABS_URL}/api{path}", headers={"Authorization": f"Bearer {ABS_TOKEN}"}, timeout=10)
    r.raise_for_status()
    return r.json()

def abs_patch(path: str, data: dict) -> dict:
    r = requests.patch(f"{ABS_URL}/api{path}", json=data,
                       headers={"Authorization": f"Bearer {ABS_TOKEN}"}, timeout=10)
    r.raise_for_status()
    return r.json() if r.content else {}

def get_libraries() -> list[dict]:
    return abs_get("/libraries").get("libraries", [])

def get_series_books(library_id: str, series_id: str, kids_only: bool = False) -> list[dict]:
    """
    Return library items belonging to a series, ordered by series sequence.
    If kids_only=True, filters to books with the ABS_KIDS_TAG tag.

    ABS does not expose a /series/{id}/books endpoint — books are fetched via
    the items endpoint with a base64-encoded series filter, which is the same
    mechanism the ABS web UI uses internally.
    """
    encoded = base64.b64encode(series_id.encode()).decode()
    data = abs_get(f"/libraries/{library_id}/items?filter=series.{encoded}&limit=500&sort=media.metadata.title")
    items = data.get("results", [])

    if kids_only:
        items = [b for b in items if _book_has_kids_tag(b)]

    def seq(item):
        """Extract the numeric series sequence for this specific series."""
        try:
            series_list = (
                item.get("media", {}).get("metadata", {}).get("series", [])
                or item.get("media", {}).get("metadata", {}).get("seriesName", [])
            )
            if isinstance(series_list, list):
                for s in series_list:
                    if isinstance(s, dict) and (s.get("id") == series_id or s.get("name")):
                        return float(s.get("sequence") or 0)
            return float(item.get("seriesSequence") or 0)
        except (ValueError, TypeError):
            return 0

    return sorted(items, key=seq)

def get_book_progress(book_id: str) -> Optional[dict]:
    try:
        return abs_get(f"/me/progress/{book_id}")
    except Exception:
        return None

def find_target_book(books: list[dict]) -> tuple[Optional[dict], bool]:
    """
    Find the book to play. Returns (book, series_restart).
    series_restart=True means all books were finished; we're looping back to book 1
    without modifying any ABS progress records.

    Priority:
    1. In-progress book (started, not finished) → resume it
    2. Next unstarted/unfinished book after the last finished one
    3. All books finished → series restart: book 1, start_time=0, series_restart=True
    4. Nothing ever played → book 1
    """
    if not books:
        return None, False

    last_finished_idx = -1
    in_progress = None
    finished_count = 0

    for i, book in enumerate(books):
        prog = get_book_progress(book["id"])
        if prog:
            if prog.get("isFinished"):
                last_finished_idx = i
                finished_count += 1
            elif prog.get("currentTime", 0) > 0:
                in_progress = book

    # 1. Resume in-progress book
    if in_progress:
        return in_progress, False

    # 2. Play next book after the last finished one
    next_idx = last_finished_idx + 1
    if next_idx < len(books):
        return books[next_idx], False

    # 3. All finished (or none started) → restart series from book 1
    all_finished = finished_count == len(books)
    log.info(
        f"Series {'fully completed' if all_finished else 'never started'} "
        f"— restarting from book 1 at 0:00 (ABS progress records untouched)"
    )
    return books[0], True

def get_stream_url(book_id: str, series_restart: bool = False) -> tuple[str, float]:
    """
    Return (stream_url, start_time_seconds).

    For normal playback: resumes from ABS currentTime.
    For a series restart (all books previously finished): also resumes from ABS
    currentTime, EXCEPT if the resume point is within the last SERIES_RESTART_TAIL_PCT
    of the book — in that case we restart from 0:00, since the book was almost
    certainly finished rather than genuinely paused near the end.
    """
    prog = get_book_progress(book_id) or {}
    current_time = prog.get("currentTime", 0) or 0
    duration = prog.get("duration", 0) or 0

    if series_restart and duration > 0:
        pct_remaining = (duration - current_time) / duration
        if pct_remaining <= SERIES_RESTART_TAIL_PCT:
            log.info(
                f"Series restart: {book_id} is {pct_remaining*100:.1f}% from end "
                f"(<= {SERIES_RESTART_TAIL_PCT*100:.0f}% threshold) — starting from 0:00"
            )
            return _build_stream_url(book_id), 0.0

    return _build_stream_url(book_id), current_time


def _build_stream_url(book_id: str) -> str:
    """Resolve the best stream URL for a book (direct file or HLS fallback)."""

    # Try to get direct play path
    try:
        item = abs_get(f"/items/{book_id}?expanded=1")
        media = item.get("media", {})
        audio_files = media.get("audioFiles", [])
        if audio_files:
            af = audio_files[0]
            ino = af.get("ino") or af.get("inode")
            if ino:
                return f"{ABS_PUBLIC_URL}/api/items/{book_id}/file/{ino}?token={ABS_TOKEN}"
    except Exception as e:
        log.warning(f"Direct play lookup failed: {e}")

    # Fallback: HLS stream
    return f"{ABS_PUBLIC_URL}/api/items/{book_id}/play?token={ABS_TOKEN}"

def update_abs_progress(book_id: str, current_time: float, duration: float):
    is_finished = duration > 0 and (current_time / duration) > 0.98
    try:
        abs_patch(f"/me/progress/{book_id}", {
            "currentTime": current_time,
            "duration": duration,
            "isFinished": is_finished,
            "lastUpdate": int(time.time() * 1000),
        })
        log.info(f"Progress saved: {book_id} @ {current_time:.1f}s / {duration:.1f}s (finished={is_finished})")
    except Exception as e:
        log.error(f"Failed to update progress for {book_id}: {e}")

# ---------------------------------------------------------------------------
# Chromecast session
# ---------------------------------------------------------------------------

@dataclass
class CastSession:
    session_id: str
    device_name: str
    book_id: str
    book_title: str
    stream_url: str
    start_time: float
    duration: float = 0.0
    current_time: float = 0.0
    state: str = "IDLE"   # IDLE | LOADING | PLAYING | PAUSED | BUFFERING | DONE | STOPPED | ERROR
    error: str = ""
    book_author: str = ""
    series_name: str = ""
    cast: object = field(default=None, repr=False)
    _stop: threading.Event = field(default_factory=threading.Event, repr=False)
    _thread: object = field(default=None, repr=False)
    _stopped_at: float = field(default=0.0, repr=False)

    def to_dict(self) -> dict:
        return {
            "session_id": self.session_id,
            "device_name": self.device_name,
            "book_id": self.book_id,
            "book_title": self.book_title,
            "book_author": self.book_author,
            "series_name": self.series_name,
            "stream_url": self.stream_url,
            "start_time": self.start_time,
            "duration": self.duration,
            "current_time": self.current_time,
            "state": self.state,
            "error": self.error,
        }

    def start(self):
        database.upsert_session(self.to_dict())
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self):
        self.state = "STOPPED"
        self._stopped_at = time.time()
        self._stop.set()
        database.upsert_session({**self.to_dict(), "stopped_at": self._stopped_at})
        if self.cast:
            try:
                self.cast.media_controller.stop()
                self.cast.quit_app()
            except Exception:
                pass

    def pause(self):
        if self.cast:
            try:
                self.cast.media_controller.pause()
                self.state = "PAUSED"
                database.upsert_session(self.to_dict())
            except Exception as e:
                log.error(f"Pause failed: {e}")

    def resume(self):
        if self.cast:
            try:
                self.cast.media_controller.play()
                self.state = "PLAYING"
                database.upsert_session(self.to_dict())
            except Exception as e:
                log.error(f"Resume failed: {e}")

    def seek(self, position: float):
        """Seek to absolute position in seconds."""
        if self.cast:
            try:
                self.cast.media_controller.seek(position)
                self.current_time = position
                database.upsert_session(self.to_dict())
            except Exception as e:
                log.error(f"Seek failed: {e}")

    def _run(self):
        browser = None
        zconf = None
        try:
            event = threading.Event()
            found_info: list[pychromecast.CastInfo] = []

            def _on_add(uuid, _service):
                ci = browser.devices.get(uuid)
                if ci and ci.friendly_name == self.device_name:
                    found_info.append(ci)
                    event.set()

            zconf = zeroconf.Zeroconf()
            browser = pychromecast.CastBrowser(
                pychromecast.SimpleCastListener(add_callback=_on_add),
                zconf,
            )
            browser.start_discovery()
            event.wait(timeout=10)
            browser.stop_discovery()
            browser = None

            if not found_info:
                self.state = "ERROR"
                self.error = f"Device '{self.device_name}' not found on network"
                return

            ci = found_info[0]
            self.cast = pychromecast.get_chromecast_from_host(
                (ci.host, ci.port, ci.uuid, ci.model_name, ci.friendly_name)
            )
            self.cast.wait()

            mc: MediaController = self.cast.media_controller
            self.state = "LOADING"

            mc.play_media(
                self.stream_url,
                content_type="audio/mpeg",
                title=self.book_title,
                current_time=self.start_time,
                autoplay=True,
            )
            mc.block_until_active(timeout=15)

            last_save = time.time()
            self.state = "PLAYING"

            while not self._stop.is_set():
                mc.update_status()
                status = mc.status

                if status:
                    self.current_time = status.adjusted_current_time or self.current_time
                    self.duration = status.duration or self.duration
                    player_state = status.player_state
                    if player_state in ("PLAYING", "BUFFERING", "PAUSED", "IDLE"):
                        self.state = player_state

                now = time.time()
                if now - last_save >= PROGRESS_INTERVAL:
                    update_abs_progress(self.book_id, self.current_time, self.duration)
                    database.upsert_session(self.to_dict())
                    last_save = now

                if self.state in ("IDLE",) and self.current_time > 10:
                    # Playback ended naturally
                    update_abs_progress(self.book_id, self.duration or self.current_time, self.duration or self.current_time)
                    self.state = "DONE"
                    self._stopped_at = time.time()
                    break

                time.sleep(2)

        except Exception as e:
            log.exception(f"CastSession error: {e}")
            self.state = "ERROR"
            self.error = str(e)
            self._stopped_at = time.time()
        finally:
            if browser:
                try:
                    browser.stop_discovery()
                except Exception:
                    pass
            if zconf:
                try:
                    zconf.close()
                except Exception:
                    pass
            if self.cast:
                try:
                    self.cast.disconnect()
                except Exception:
                    pass

# ---------------------------------------------------------------------------
# Device Discovery
# ---------------------------------------------------------------------------

def discover_devices(timeout: int = 5) -> list[dict]:
    global discovered_devices
    log.info("Discovering Chromecast devices...")

    zconf = zeroconf.Zeroconf()
    # browser must be defined before the listener callback closes over it
    browser = pychromecast.CastBrowser(
        pychromecast.SimpleCastListener(
            add_callback=lambda uuid, _service: None  # placeholder; we read devices after timeout
        ),
        zconf,
    )
    browser.start_discovery()
    time.sleep(timeout)
    browser.stop_discovery()
    zconf.close()

    devices = [
        {
            "name": ci.friendly_name,
            "host": ci.host,
            "port": ci.port,
            "model": ci.model_name or "Unknown",
        }
        for ci in browser.devices.values()
        if ci.friendly_name  # skip any partially-discovered entries
    ]
    with discovery_lock:
        discovered_devices = devices
    log.info(f"Found {len(devices)} devices: {[d['name'] for d in devices]}")
    return devices


def fuzzy_match_device(query: str, devices: list[dict], cutoff: float = 0.4) -> Optional[dict]:
    """
    Find the best-matching device for a fuzzy name query.
    Matching strategy (in order):
      1. Exact match (case-insensitive)
      2. Query is a substring of a device name (case-insensitive)
      3. Device name is a substring of the query (case-insensitive)
      4. difflib SequenceMatcher ratio >= cutoff (best score wins)
    Returns the matched device dict, or None if no match found.
    """
    if not devices:
        return None

    q = query.strip().lower()

    # 1. Exact
    for d in devices:
        if d["name"].lower() == q:
            log.info(f"Fuzzy match: exact '{d['name']}' for query '{query}'")
            return d

    # 2. Query substring of device name
    for d in devices:
        if q in d["name"].lower():
            log.info(f"Fuzzy match: substring '{d['name']}' for query '{query}'")
            return d

    # 3. Device name substring of query
    for d in devices:
        if d["name"].lower() in q:
            log.info(f"Fuzzy match: reverse-substring '{d['name']}' for query '{query}'")
            return d

    # 4. SequenceMatcher best ratio
    scored = [
        (difflib.SequenceMatcher(None, q, d["name"].lower()).ratio(), d)
        for d in devices
    ]
    scored.sort(key=lambda x: x[0], reverse=True)
    best_score, best_device = scored[0]
    if best_score >= cutoff:
        log.info(f"Fuzzy match: ratio {best_score:.2f} → '{best_device['name']}' for query '{query}'")
        return best_device

    log.warning(f"Fuzzy match: no match for '{query}' (best score {best_score:.2f} < {cutoff})")
    return None


def resolve_device_name(query: Optional[str]) -> tuple[str, str]:
    """
    Resolve a (possibly fuzzy) device name query to an exact friendly name.
    Falls back to DEFAULT_DEVICE if query is empty/None.
    Returns (resolved_name, matched_from) where matched_from is the original query.
    Raises ValueError if no match can be found.
    """
    effective_query = (query or DEFAULT_DEVICE or "").strip()
    if not effective_query:
        raise ValueError("No device name provided and no DEFAULT_DEVICE configured")

    # Use cached devices if available, otherwise do a quick discovery
    with discovery_lock:
        devices = list(discovered_devices)
    if not devices:
        devices = discover_devices(timeout=5)

    match = fuzzy_match_device(effective_query, devices)
    if not match:
        available = [d["name"] for d in devices]
        raise ValueError(
            f"No device matching '{effective_query}'. Available: {available}"
        )
    return match["name"], effective_query

# ---------------------------------------------------------------------------
# Flask Routes
# ---------------------------------------------------------------------------

@app.get("/api/devices")
def api_devices():
    force = request.args.get("refresh") == "1"
    if force or database.devices_stale():
        _refresh_devices(force=True)
    devices = database.get_devices_cached()
    return jsonify(devices)


@app.get("/api/config")
def api_config():
    """Return current server-side config (non-sensitive fields only)."""
    return jsonify({
        "default_device": DEFAULT_DEVICE,
        "progress_interval": PROGRESS_INTERVAL,
        "abs_url": ABS_URL,
        "abs_public_url": ABS_PUBLIC_URL,
        "series_restart_tail_pct": SERIES_RESTART_TAIL_PCT,
        "kids_tag": ABS_KIDS_TAG,
    })


@app.post("/api/resolve-device")
def api_resolve_device():
    """Resolve a fuzzy device name query without starting a cast session."""
    query = (request.json or {}).get("query", "")
    try:
        with discovery_lock:
            devices = list(discovered_devices)
        if not devices:
            devices = discover_devices()
        match = fuzzy_match_device(query or DEFAULT_DEVICE or "", devices)
        if not match:
            return jsonify({"error": "No matching device found"}), 404
        return jsonify({"resolved": match, "query": query})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.get("/api/libraries")
def api_libraries():
    try:
        force = request.args.get("refresh") == "1"
        if force or database.libraries_stale():
            _refresh_libraries_and_series(force=force)
        return jsonify(database.get_libraries_cached())
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.get("/api/libraries/<library_id>/series")
def api_series(library_id):
    try:
        force = request.args.get("refresh") == "1"
        if force or database.series_stale(library_id):
            _refresh_libraries_and_series(force=force)
        cached = database.get_series_cached(library_id)
        # Return shape the UI expects: id + name
        return jsonify([{"id": s["id"], "name": s["name"]} for s in cached])
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.get("/api/libraries/<library_id>/series/<series_id>/books")
def api_series_books(library_id, series_id):
    try:
        books = get_series_books(library_id, series_id)
        target, series_restart = find_target_book(books)
        return jsonify({
            "books": [{"id": b["id"], "title": b.get("media", {}).get("metadata", {}).get("title", "Unknown")} for b in books],
            "target_book_id": target["id"] if target else None,
            "series_restart": series_restart,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.post("/api/cast")
def api_cast():
    """
    Body: { library_id, series_id, device_name (fuzzy, optional if DEFAULT_DEVICE set), book_id (optional override) }
    Also accepts ?device=<fuzzy_name> as a query param for quick CLI-style invocation.
    """
    body = request.json or {}
    library_id = body.get("library_id")
    series_id = body.get("series_id")
    # device_name: body > query param > DEFAULT_DEVICE (resolved fuzzy in all cases)
    device_query = body.get("device_name") or request.args.get("device") or ""
    book_id_override = body.get("book_id")
    kids_only = bool(body.get("kids_only", False))

    if not all([library_id, series_id]):
        return jsonify({"error": "library_id and series_id are required"}), 400

    try:
        resolved_device, original_query = resolve_device_name(device_query)
    except ValueError as e:
        return jsonify({"error": str(e)}), 400

    try:
        books = get_series_books(library_id, series_id, kids_only=kids_only)
        series_restart = False
        if book_id_override:
            target = next((b for b in books if b["id"] == book_id_override), None)
        else:
            target, series_restart = find_target_book(books)

        if not target:
            return jsonify({"error": "No book found in series"}), 404

        book_title = target.get("media", {}).get("metadata", {}).get("title", "Unknown")
        book_author = target.get("media", {}).get("metadata", {}).get("authorName", "")
        # Extract series name robustly
        series_raw = target.get("media", {}).get("metadata", {}).get("series")
        series_display = ""
        if isinstance(series_raw, str):
            series_display = series_raw
        elif isinstance(series_raw, list) and series_raw:
            first = series_raw[0]
            series_display = first.get("name", "") if isinstance(first, dict) else str(first)
        elif isinstance(series_raw, dict):
            first = next(iter(series_raw.values()), None)
            series_display = first.get("name", "") if isinstance(first, dict) else str(first or "")
        stream_url, start_time = get_stream_url(target["id"], series_restart=series_restart)

        session = CastSession(
            session_id=str(uuid.uuid4())[:8],
            device_name=resolved_device,
            book_id=target["id"],
            book_title=book_title,
            book_author=book_author,
            series_name=series_display,
            stream_url=stream_url,
            start_time=start_time,
        )
        cast_sessions[session.session_id] = session
        session.start()

        resp = session.to_dict()
        resp["device_query"] = original_query
        resp["series_restart"] = series_restart
        return jsonify(resp), 201

    except Exception as e:
        log.exception("Cast error")
        return jsonify({"error": str(e)}), 500

TERMINAL_STATES = {"DONE", "STOPPED", "ERROR"}
SESSION_EVICT_AFTER = 30  # seconds — keep terminal sessions visible briefly then drop them

def _evict_terminal_sessions():
    """Remove sessions that have been in a terminal state for more than SESSION_EVICT_AFTER seconds."""
    now = time.time()
    to_delete = [
        sid for sid, s in cast_sessions.items()
        if s.state in TERMINAL_STATES and s._stopped_at > 0 and (now - s._stopped_at) > SESSION_EVICT_AFTER
    ]
    for sid in to_delete:
        del cast_sessions[sid]
        log.info(f"Evicted terminal session {sid}")

@app.get("/api/sessions/history")
def api_session_history():
    limit = int(request.args.get("limit", 50))
    return jsonify(database.get_session_history(limit))


@app.get("/api/sessions")
def api_sessions():
    _evict_terminal_sessions()
    return jsonify([s.to_dict() for s in cast_sessions.values()])

@app.get("/api/sessions/<session_id>")
def api_session(session_id):
    s = cast_sessions.get(session_id)
    if not s:
        return jsonify({"error": "Not found"}), 404
    return jsonify(s.to_dict())

@app.post("/api/sessions/<session_id>/stop")
def api_stop(session_id):
    s = cast_sessions.get(session_id)
    if not s:
        return jsonify({"error": "Not found"}), 404
    s.stop()
    del cast_sessions[session_id]
    return jsonify({"ok": True})

@app.post("/api/sessions/<session_id>/pause")
def api_pause(session_id):
    s = cast_sessions.get(session_id)
    if not s:
        return jsonify({"error": "Not found"}), 404
    s.pause()
    return jsonify(s.to_dict())

@app.post("/api/sessions/<session_id>/resume")
def api_resume(session_id):
    s = cast_sessions.get(session_id)
    if not s:
        return jsonify({"error": "Not found"}), 404
    s.resume()
    return jsonify(s.to_dict())

@app.post("/api/sessions/<session_id>/seek")
def api_seek(session_id):
    s = cast_sessions.get(session_id)
    if not s:
        return jsonify({"error": "Not found"}), 404
    body = request.json or {}
    position = body.get("position")
    if position is None:
        return jsonify({"error": "position required"}), 400
    s.seek(float(position))
    return jsonify(s.to_dict())


# ---------------------------------------------------------------------------
# QR Code cast endpoint
# GET /play?library=<library_id>&series=<series_id>&device=<fuzzy_name>
#
# Designed to be the target of a QR code. Opens in any browser, immediately
# starts casting, and returns a confirmation page. device is optional if
# DEFAULT_DEVICE is configured.
# ---------------------------------------------------------------------------

import os as _os
_HERE = _os.path.dirname(_os.path.abspath(__file__))

def _render_qr_page(icon, status_label, status_color, heading, subtitle, extra_html=""):
    from flask import render_template_string
    template_path = _os.path.join(_HERE, "templates", "play.html")
    with open(template_path, "r") as f:
        template_str = f.read()
    return render_template_string(
        template_str,
        icon=icon,
        status_label=status_label,
        status_color=status_color,
        heading=heading,
        subtitle=subtitle,
        extra_html=extra_html,
    )


@app.get("/play")
def qr_play():
    """
    QR code cast trigger.
    Query params:
      library  — ABS library ID (required)
      series   — ABS series ID (required)
      device   — fuzzy device name (optional if DEFAULT_DEVICE is set)
    """
    library_id = request.args.get("library", "").strip()
    series_id  = request.args.get("series", "").strip()
    device_query = request.args.get("device", "").strip()

    if not library_id or not series_id:
        return _render_qr_page(
            "⚠️", "Configuration Error", "#f04a6a",
            "Missing Parameters",
            "This QR code is missing library or series parameters.",
            '<div class="error-box">Expected: /play?library=&lt;id&gt;&amp;series=&lt;id&gt;</div>',
        ), 400

    # Resolve device
    try:
        resolved_device, _ = resolve_device_name(device_query)
    except ValueError as e:
        return _render_qr_page(
            "📡", "Device Error", "#f04a6a",
            "Device Not Found",
            "Could not find a matching Chromecast on the network.",
            f'<div class="error-box">{str(e)}</div>',
        ), 404

    # Find target book
    try:
        books = get_series_books(library_id, series_id, kids_only=bool(ABS_KIDS_TAG))
        if not books:
            return _render_qr_page(
                "📚", "Not Found", "#f04a6a",
                "No Books Found",
                "No books were found for this series in AudioBookshelf.",
                f'<div class="error-box">library={library_id}<br>series={series_id}</div>',
            ), 404

        target, series_restart = find_target_book(books)
        if not target:
            return _render_qr_page(
                "📚", "Not Found", "#f04a6a",
                "No Book to Play",
                "Could not determine which book to play next.",
            ), 404

        book_title = target.get("media", {}).get("metadata", {}).get("title", "Unknown")
        book_author = target.get("media", {}).get("metadata", {}).get("authorName", "")
        series_raw = books[0].get("media", {}).get("metadata", {}).get("series") if books else None
        log.debug(f"series_raw type={type(series_raw).__name__} value={series_raw!r}")
        series_name = ""
        if isinstance(series_raw, str):
            series_name = series_raw
        elif isinstance(series_raw, dict):
            first = next(iter(series_raw.values()), None)
            if isinstance(first, dict):
                series_name = first.get("name", "")
            elif isinstance(first, str):
                series_name = first
        elif isinstance(series_raw, list) and series_raw:
            first = series_raw[0]
            if isinstance(first, dict):
                series_name = first.get("name", "")
            elif isinstance(first, str):
                series_name = first
        stream_url, start_time = get_stream_url(target["id"], series_restart=series_restart)

        session = CastSession(
            session_id=str(uuid.uuid4())[:8],
            device_name=resolved_device,
            book_id=target["id"],
            book_title=book_title,
            book_author=book_author,
            series_name=series_name,
            stream_url=stream_url,
            start_time=start_time,
        )
        cast_sessions[session.session_id] = session
        session.start()

        # Build resume info line
        if series_restart:
            resume_line = '<div class="resume-info">↺ Series restart — beginning from book 1</div>'
        elif start_time > 60:
            h = int(start_time // 3600)
            m = int((start_time % 3600) // 60)
            s = int(start_time % 60)
            ts = f"{h}:{m:02d}:{s:02d}" if h else f"{m}:{s:02d}"
            resume_line = f'<div class="resume-info">Resuming from <span>{ts}</span></div>'
        else:
            resume_line = '<div class="resume-info">Starting from the beginning</div>'

        subtitle = series_name if series_name else "AudioBookshelf"
        extra = f"""
            <div class="device-badge">📡 {resolved_device}</div>
            {resume_line}
        """

        return _render_qr_page(
            "🎧", "Now Casting", "#4af0a0",
            book_title,
            subtitle,
            extra,
        )

    except Exception as e:
        log.exception("QR play error")
        return _render_qr_page(
            "💥", "Error", "#f04a6a",
            "Something Went Wrong",
            "An unexpected error occurred while starting playback.",
            f'<div class="error-box">{str(e)}</div>',
        ), 500


@app.get("/api/series-lookup")
def api_series_lookup():
    """
    Returns all series across all libraries from cache (with cover URLs and QR URLs).
    Pass ?refresh=1 to force a fresh pull from ABS.
    """
    force = request.args.get("refresh") == "1"
    q = request.args.get("q", "").strip().lower()

    if force or database.series_stale():
        _refresh_libraries_and_series(force=force)

    results = database.get_series_cached()
    if q:
        results = [s for s in results if q in s["name"].lower()]

    # Normalise field names: DB uses "id"/"name", frontend expects "series_id"/"series_name"
    normalised = []
    for s in results:
        normalised.append({
            "series_id":    s["id"],
            "series_name":  s["name"],
            "library_id":   s["library_id"],
            "library_name": s.get("library_name", ""),
            "book_count":   s.get("book_count", 0),
            "cover_item_id": s.get("cover_item_id"),
            "cover_url":    s.get("cover_url"),
            "qr_url":       s.get("qr_url"),
        })
    return jsonify(normalised)


@app.get("/api/cache/status")
def api_cache_status():
    """Return cache staleness info for the dashboard."""
    conn = database.get_conn()
    lib_row   = conn.execute("SELECT COUNT(*) as n, MIN(refreshed_at) as oldest FROM libraries").fetchone()
    ser_row   = conn.execute("SELECT COUNT(*) as n, MIN(refreshed_at) as oldest FROM series").fetchone()
    dev_row   = conn.execute("SELECT COUNT(*) as n, MIN(refreshed_at) as oldest FROM devices").fetchone()
    sess_row  = conn.execute("SELECT COUNT(*) as n FROM sessions").fetchone()
    return jsonify({
        "libraries":  {"count": lib_row["n"], "stale": database.libraries_stale(), "oldest": lib_row["oldest"]},
        "series":     {"count": ser_row["n"], "stale": database.series_stale(),    "oldest": ser_row["oldest"]},
        "devices":    {"count": dev_row["n"], "stale": database.devices_stale(),   "oldest": dev_row["oldest"]},
        "sessions":   {"total": sess_row["n"]},
        "ttls": {
            "libraries_s": database.CACHE_TTL_LIBRARIES,
            "series_s":    database.CACHE_TTL_SERIES,
            "devices_s":   database.CACHE_TTL_DEVICES,
        }
    })


@app.post("/api/cache/refresh")
def api_cache_refresh():
    """Force a full cache refresh (libraries, series, devices) in the background."""
    def _do():
        _refresh_libraries_and_series(force=True)
        _refresh_devices(force=True)
    threading.Thread(target=_do, daemon=True).start()
    return jsonify({"ok": True, "message": "Refresh started in background"})


@app.get("/series")
def series_page():
    from flask import send_from_directory
    return send_from_directory("static", "series.html")

@app.get("/")
def index():
    from flask import send_from_directory
    return send_from_directory("static", "index.html")

@app.get("/health")
def health():
    return jsonify({"status": "ok", "abs_url": ABS_URL, "default_device": DEFAULT_DEVICE or None})

if __name__ == "__main__":
    log.info(f"Starting ABS Cast Bridge on {HOST_IP}:{PORT}")
    log.info(f"ABS URL: {ABS_URL}")
    # Initialise database
    database.init_db()
    # Start background cache refresh thread
    refresh_thread = threading.Thread(target=_background_refresh, daemon=True, name="cache-refresh")
    refresh_thread.start()
    app.run(host=HOST_IP, port=PORT, debug=False)