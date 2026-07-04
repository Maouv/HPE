# HPE — Hermes Playwright Extension

Browser extension yang jual diri sebagai Chrome remote debugging instance.
Playwright di sisi Hermes server `connect_over_cdp` → kontrol browser HP user.

## Kenapa?

- IP HP asli → bypass anti-bot
- Session real → cookies, localStorage, login state kepake
- Gratis → nggak bayar Browserbase
- Full Playwright API

## Quick Start

```bash
# Setup
python3 -m venv venv
source venv/bin/activate
pip install fastapi uvicorn websockets playwright
playwright install chromium

# Run gateway
python -m gateway.server

# Load extension di Kiwi/Mises
# chrome://extensions → Developer mode → Load unpacked → pilih folder extension/

# Test
python tests/test_playwright.py
```

## Struktur

```
hpe/
├── gateway/        # FastAPI + WS + /json emulasi
├── extension/      # MV3 extension (chrome.debugger)
├── tests/          # Playwright end-to-end test
└── plans/          # Plan docs
```

## Network Mode

- **tailscale** — gateway listen 127.0.0.1, cuma reachable via tailnet
- **direct** — gateway listen 0.0.0.0, user buka port sendiri

## Browser Support

- Kiwi Browser (Android) ✅
- Mises Browser (Android) ✅

## License

MIT
