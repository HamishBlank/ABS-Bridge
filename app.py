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
    """
    Return library items belonging to a series, ordered by series sequence.

    ABS does not expose a /series/{id}/books endpoint — books are fetched via
    the items endpoint with a base64-encoded series filter, which is the same
    mechanism the ABS web UI uses internally.
    """
    # ABS filter format: "series.BASE64(series_id)"
    encoded = base64.b64encode(series_id.encode()).decode()
    data = abs_get(f"/libraries/{library_id}/items?filter=series.{encoded}&limit=500&sort=media.metadata.title")
    items = data.get("results", [])

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
            # Fallback: top-level seriesSequence sometimes present on item
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
        # ABS returns { results: [{id, name, nameIgnorePrefix, ...}], total, ... }
        data = abs_get(f"/libraries/{library_id}/series?limit=500&sort=name")
        results = data.get("results", [])
        # Normalise to just id + name for the UI
        series = [{"id": s["id"], "name": s.get("name") or s.get("nameIgnorePrefix") or s["id"]} for s in results]
        return jsonify(series)
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


# ---------------------------------------------------------------------------
# QR Code cast endpoint
# GET /play?library=<library_id>&series=<series_id>&device=<fuzzy_name>
#
# Designed to be the target of a QR code. Opens in any browser, immediately
# starts casting, and returns a confirmation page. device is optional if
# DEFAULT_DEVICE is configured.
# ---------------------------------------------------------------------------

QR_PAGE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{title}</title>
<link href="https://fonts.googleapis.com/css2?family=Syne:wght@400;700;800&family=Space+Mono&display=swap" rel="stylesheet">
<style>
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{
    background: #0a0a0f;
    color: #e8e8f0;
    font-family: 'Syne', sans-serif;
    min-height: 100vh;
    display: flex; align-items: center; justify-content: center;
    padding: 24px;
  }}
  .card {{
    background: #111118;
    border: 1px solid #2a2a3a;
    border-radius: 16px;
    padding: 40px 36px;
    max-width: 420px;
    width: 100%;
    text-align: center;
  }}
  .icon {{ font-size: 48px; margin-bottom: 20px; }}
  .status {{
    font-size: 11px; font-weight: 700;
    text-transform: uppercase; letter-spacing: 3px;
    font-family: 'Space Mono', monospace;
    margin-bottom: 16px;
    color: {status_color};
  }}
  h1 {{
    font-size: 22px; font-weight: 800;
    line-height: 1.2; margin-bottom: 10px;
  }}
  .subtitle {{
    font-size: 13px; color: #666680;
    font-family: 'Space Mono', monospace;
    line-height: 1.6;
    margin-bottom: 28px;
  }}
  .device-badge {{
    display: inline-flex; align-items: center; gap: 8px;
    background: rgba(124,106,247,0.1);
    border: 1px solid rgba(124,106,247,0.25);
    border-radius: 20px;
    padding: 8px 16px;
    font-size: 13px; font-weight: 600;
    color: #a090ff;
    margin-bottom: 28px;
  }}
  .error-box {{
    background: rgba(240,74,106,0.1);
    border: 1px solid rgba(240,74,106,0.3);
    border-radius: 8px;
    padding: 14px;
    font-size: 13px; color: #f04a6a;
    font-family: 'Space Mono', monospace;
    line-height: 1.5;
    margin-bottom: 24px;
    text-align: left;
  }}
  .resume-info {{
    font-size: 12px; color: #666680;
    font-family: 'Space Mono', monospace;
  }}
  .resume-info span {{ color: #4af0a0; }}
  a {{
    color: #7c6af7; text-decoration: none;
    font-size: 13px;
    display: block; margin-top: 24px;
    font-family: 'Space Mono', monospace;
  }}
  a:hover {{ text-decoration: underline; }}
</style>
</head>
<body>
<div class="card">
  <div class="icon">{icon}</div>
  <div class="status">{status_label}</div>
  <h1>{heading}</h1>
  <div class="subtitle">{subtitle}</div>
  {extra_html}
  <a href="/">← Open dashboard</a>
</div>
</body>
</html>"""


def _render_qr_page(icon, status_label, status_color, heading, subtitle, extra_html=""):
    return QR_PAGE.format(
        title=heading,
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
        books = get_series_books(library_id, series_id)
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
        series_name = (
            books[0].get("media", {}).get("metadata", {}).get("series", [{}])[0].get("name", "")
            if books else ""
        )
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
    Helper endpoint for building QR code URLs.
    Returns all series across all libraries with their IDs and a ready-made /play URL.
    Optionally filter by ?q=<fuzzy name>.
    """
    q = request.args.get("q", "").strip().lower()
    device = request.args.get("device", DEFAULT_DEVICE or "").strip()

    try:
        libraries = get_libraries()
        results = []
        for lib in libraries:
            try:
                series_list = abs_get(f"/libraries/{lib['id']}/series?limit=500&sort=name").get("results", [])
                for s in series_list:
                    name = s.get("name") or s.get("nameIgnorePrefix") or s["id"]
                    if q and q not in name.lower():
                        continue
                    params = f"library={lib['id']}&series={s['id']}"
                    if device:
                        params += f"&device={device}"
                    results.append({
                        "library_id": lib["id"],
                        "library_name": lib.get("name", ""),
                        "series_id": s["id"],
                        "series_name": name,
                        "qr_url": f"/play?{params}",
                        "book_count": s.get("numBooks", "?"),
                    })
            except Exception as e:
                log.warning(f"Series lookup failed for library {lib['id']}: {e}")
        return jsonify(results)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


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