#!/usr/bin/env python3
"""
AudioBookshelf Chromecast Bridge
Streams audiobooks from AudioBookshelf to Chromecast devices,
monitoring playback progress and syncing back to ABS.
"""

import asyncio
import difflib
import json
import logging
import os
import threading
import time
from dataclasses import dataclass, field, asdict
from typing import Optional
import uuid

import pychromecast
import requests
from flask import Flask, jsonify, request, Response
from flask_cors import CORS
from pychromecast.controllers.media import MediaController

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

app = Flask(__name__, static_folder="static", static_url_path="/static")
CORS(app)

# ---------------------------------------------------------------------------
# Config (env vars or defaults)
# ---------------------------------------------------------------------------
ABS_URL = os.getenv("ABS_URL", "http://localhost:13378")
ABS_TOKEN = os.getenv("ABS_TOKEN", "")
PROGRESS_INTERVAL = int(os.getenv("PROGRESS_INTERVAL", "15"))   # seconds
HOST_IP = os.getenv("HOST_IP", "0.0.0.0")
PORT = int(os.getenv("PORT", "8123"))
DEFAULT_DEVICE = os.getenv("DEFAULT_DEVICE", "")   # friendly name of preferred Chromecast
SERIES_RESTART_TAIL_PCT = float(os.getenv("SERIES_RESTART_TAIL_PCT", "0.05"))  # 0.0–1.0; default 5%

# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------
cast_sessions: dict[str, "CastSession"] = {}
discovered_devices: list[dict] = []
discovery_lock = threading.Lock()

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
    return r.json()

def get_libraries() -> list[dict]:
    return abs_get("/libraries").get("libraries", [])

def get_series_books(library_id: str, series_id: str) -> list[dict]:
    """Return books in a series, ordered by series sequence."""
    data = abs_get(f"/libraries/{library_id}/series/{series_id}")
    books = data.get("books", [])
    # Sort by series sequence
    def seq(b):
        try:
            return float(b.get("seriesSequence") or b.get("sequence") or 0)
        except (ValueError, TypeError):
            return 0
    return sorted(books, key=seq)

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
                return f"{ABS_URL}/api/items/{book_id}/file/{ino}?token={ABS_TOKEN}"
    except Exception as e:
        log.warning(f"Direct play lookup failed: {e}")

    # Fallback: HLS stream
    return f"{ABS_URL}/api/items/{book_id}/play?token={ABS_TOKEN}"

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
    state: str = "IDLE"   # IDLE | LOADING | PLAYING | PAUSED | BUFFERING | DONE | ERROR
    error: str = ""
    cast: object = field(default=None, repr=False)
    _stop: threading.Event = field(default_factory=threading.Event, repr=False)
    _thread: object = field(default=None, repr=False)

    def to_dict(self) -> dict:
        return {
            "session_id": self.session_id,
            "device_name": self.device_name,
            "book_id": self.book_id,
            "book_title": self.book_title,
            "stream_url": self.stream_url,
            "start_time": self.start_time,
            "duration": self.duration,
            "current_time": self.current_time,
            "state": self.state,
            "error": self.error,
        }

    def start(self):
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self):
        self._stop.set()
        if self.cast:
            try:
                self.cast.media_controller.stop()
                self.cast.quit_app()
            except Exception:
                pass

    def _run(self):
        try:
            chromecasts, browser = pychromecast.get_listed_chromecasts(
                friendly_names=[self.device_name]
            )
            if not chromecasts:
                self.state = "ERROR"
                self.error = f"Device '{self.device_name}' not found"
                pychromecast.discovery.stop_discovery(browser)
                return

            self.cast = chromecasts[0]
            pychromecast.discovery.stop_discovery(browser)
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
                    last_save = now

                if self.state in ("IDLE",) and self.current_time > 10:
                    # Playback ended naturally
                    update_abs_progress(self.book_id, self.duration or self.current_time, self.duration or self.current_time)
                    self.state = "DONE"
                    break

                time.sleep(2)

        except Exception as e:
            log.exception(f"CastSession error: {e}")
            self.state = "ERROR"
            self.error = str(e)
        finally:
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
    services, browser = pychromecast.discovery.discover_chromecasts(max_devices=None, timeout=timeout)
    pychromecast.discovery.stop_discovery(browser)
    devices = []
    for svc in services:
        devices.append({
            "name": svc.friendly_name,
            "host": svc.host,
            "port": svc.port,
            "model": getattr(svc, "model_name", "Unknown"),
        })
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
    devices = discover_devices()
    # Mark which device is the default (fuzzy match against DEFAULT_DEVICE)
    if DEFAULT_DEVICE:
        match = fuzzy_match_device(DEFAULT_DEVICE, devices)
        for d in devices:
            d["is_default"] = (match is not None and d["name"] == match["name"])
    else:
        for d in devices:
            d["is_default"] = False
    return jsonify(devices)


@app.get("/api/config")
def api_config():
    """Return current server-side config (non-sensitive fields only)."""
    return jsonify({
        "default_device": DEFAULT_DEVICE,
        "progress_interval": PROGRESS_INTERVAL,
        "abs_url": ABS_URL,
        "series_restart_tail_pct": SERIES_RESTART_TAIL_PCT,
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
        return jsonify(get_libraries())
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.get("/api/libraries/<library_id>/series")
def api_series(library_id):
    try:
        data = abs_get(f"/libraries/{library_id}/series?limit=100")
        return jsonify(data.get("results", []))
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

    if not all([library_id, series_id]):
        return jsonify({"error": "library_id and series_id are required"}), 400

    try:
        resolved_device, original_query = resolve_device_name(device_query)
    except ValueError as e:
        return jsonify({"error": str(e)}), 400

    try:
        books = get_series_books(library_id, series_id)
        series_restart = False
        if book_id_override:
            target = next((b for b in books if b["id"] == book_id_override), None)
        else:
            target, series_restart = find_target_book(books)

        if not target:
            return jsonify({"error": "No book found in series"}), 404

        book_title = target.get("media", {}).get("metadata", {}).get("title", "Unknown")
        stream_url, start_time = get_stream_url(target["id"], series_restart=series_restart)

        session = CastSession(
            session_id=str(uuid.uuid4())[:8],
            device_name=resolved_device,
            book_id=target["id"],
            book_title=book_title,
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

@app.get("/api/sessions")
def api_sessions():
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
    return jsonify({"ok": True})

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
    app.run(host=HOST_IP, port=PORT, debug=False)