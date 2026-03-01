# ABS Cast Bridge

Stream AudioBookshelf audiobook series to Chromecast devices, with automatic progress tracking and sync.

## Features

- 🎯 **Smart book selection**: Plays in-progress book, or next unstarted book, or first in series
- 📡 **Chromecast streaming**: Direct-play or HLS transcoded audio streamed to any Chromecast/Google Home
- 🔄 **Progress sync**: Updates AudioBookshelf progress every 15 seconds and on completion
- 🌐 **Web UI**: Browser-based dashboard to browse series, pick devices, and monitor playback
- 🐳 **Docker ready**: Single container with `network_mode: host` for mDNS discovery

---

## Quick Start

### Option A: Docker Compose (recommended)

```bash
# Clone / copy files
git clone <repo> abs-cast-bridge && cd abs-cast-bridge

# Create .env
cat > .env <<EOF
ABS_URL=http://192.168.1.100:13378
ABS_TOKEN=your-token-here
EOF

docker compose up -d
```

Open http://localhost:8123 in your browser.

> **Important**: `network_mode: host` is required for Chromecast mDNS discovery on Linux.
> On macOS/Windows Docker Desktop, you may need to run natively instead.

### Option B: Run natively

```bash
pip install -r requirements.txt

export ABS_URL=http://192.168.1.100:13378
export ABS_TOKEN=your-token-here

python app.py
```

---

## Getting your ABS Token

1. Open AudioBookshelf → **Settings → Users**
2. Click your username
3. Copy the **API Token**

---

## Environment Variables

| Variable           | Default                    | Description                          |
|--------------------|----------------------------|--------------------------------------|
| `ABS_URL`          | `http://localhost:13378`   | AudioBookshelf server URL            |
| `ABS_TOKEN`        | *(required)*               | ABS API token                        |
| `PROGRESS_INTERVAL`| `15`                       | Seconds between progress saves       |
| `HOST_IP`          | `0.0.0.0`                  | Interface to bind Flask              |
| `PORT`             | `8123`                     | Port to serve on                     |

---

## How It Works

```
Browser UI
    │
    ▼
Flask API (app.py)
    │
    ├── GET /api/libraries          → ABS: list libraries
    ├── GET /api/libraries/:id/series → ABS: list series
    ├── GET /api/.../books          → ABS: books + progress → picks target
    ├── POST /api/cast              → starts CastSession thread
    └── GET /api/sessions           → session status poll
              │
              ▼
       CastSession (thread)
              │
              ├── pychromecast: connects to device
              ├── play_media(stream_url, start_time=lastPosition)
              └── every 15s: PATCH /api/me/progress/:bookId → ABS
```

### Book Selection Logic

1. Any book in the series with `currentTime > 0` and not finished → **play that one**
2. Last finished book's index + 1 → **play next**
3. Fallback → **play first book**

---

## REST API

```
GET  /health
GET  /api/devices                           → scan Chromecast devices
GET  /api/libraries
GET  /api/libraries/:lib_id/series
GET  /api/libraries/:lib_id/series/:sid/books
POST /api/cast  { library_id, series_id, device_name, book_id? }
GET  /api/sessions
GET  /api/sessions/:id
POST /api/sessions/:id/stop
```

---

## Notes

- **macOS Docker**: Chromecast discovery relies on mDNS (port 5353). Docker on macOS doesn't support `network_mode: host`, so run `python app.py` directly on macOS.
- **Audio format**: ABS direct-play sends the original audio file. Most Chromecasts support MP3, AAC, FLAC, and Opus. If your files are in an unsupported format, use ABS's transcoding endpoint.
- **Multiple books open**: Only one CastSession per device is tracked. Starting a new cast for the same device creates a new session; stop the old one first.
