# HPE — Hermes Playwright Extension

> Browser extension yang jual diri sebagai Chrome remote debugging instance, sehingga Playwright di sisi Hermes server bisa `connect_over_cdp` dan kontrol browser di HP user secara penuh.

## Kenapa?

- IP HP asli → bypass anti-bot (Cloudflare, CAPTCHA, IP block)
- Session real → cookies, localStorage, login state kepake
- Gratis → nggak bayar Browserbase
- Full Playwright API → locator, auto-wait, network intercept, evaluate

## Arsitektur

```
Hermes Server                              HP Android
┌────────────────────┐                    ┌────────────────────┐
│                    │                    │ Kiwi / Mises       │
│  Playwright  ──────┼── connect_over_cdp │  ┌──────────────┐  │
│       │            │                    │  │ Extension    │  │
│       ▼            │                    │  │ (MV3)        │  │
│  Gateway           │◄──── WebSocket ────┼──│              │  │
│  FastAPI           │     (TLS/plain)    │  │ chrome.      │  │
│  - /json/version   │                    │  │ debugger     │  │
│  - /json           │                    │  │ chrome.tabs  │  │
│  - /json/new       │                    │  └──────────────┘  │
│  - WS /devtools/.. │                    │                    │
│                    │                    │                    │
└────────────────────┘                    └────────────────────┘
```

## Network Mode

```yaml
# config.yaml
gateway:
  platforms:
    browser-ext:
      network_mode: "tailscale"   # atau "direct"
      # tailscale: listen 127.0.0.1, auto-detect tailnet IP
      # direct: listen 0.0.0.0, user tanggung jawab security sendiri
      ws_port: 8765
      password: ""                # optional second factor
```

- **tailscale** — gateway listen `127.0.0.1:8765`, cuma reachable via tailnet mesh. Zero port exposure
- **direct** — gateway listen `0.0.0.0:8765`, user buka port sendiri (port forwarding / reverse proxy). User tanggung jawab security

## Dependencies

```bash
python3 -m venv venv
source venv/bin/activate
pip install fastapi uvicorn websockets playwright
playwright install chromium  # untuk testing di server side
```

## Struktur Project

```
hpe/
├── venv/                       # Python virtualenv (gitignore)
├── gateway/
│   ├── __init__.py
│   ├── server.py               # FastAPI app + /json endpoints
│   ├── cdp_relay.py            # WebSocket relay logic
│   ├── auth.py                 # Password auth (optional)
│   └── config.py               # Config loader
├── extension/
│   ├── manifest.json           # MV3, debugger permission
│   ├── background.js           # WS client + chrome.debugger bridge
│   ├── popup.html              # Connect/disconnect UI
│   ├── popup.js                # Popup logic
│   └── icons/                  # Extension icons
├── tests/
│   ├── test_playwright.py      # connect_over_cdp → goto → screenshot
│   ├── test_cdp_relay.py       # WS relay unit tests
│   └── test_json_endpoints.py  # /json emulasi tests
├── plans/
│   └── PLAN.md                 # File ini
├── .gitignore
└── README.md
```

---

## POC — Fase 1 (3-5 hari)

### Goal

Buktikan 3 asumsi paling berisiko:

1. `chrome.debugger` API jalan di Kiwi/Mises Android
2. Playwright `connect_over_cdp` accept fake `/json` endpoint
3. CDP relay via WebSocket jalan tanpa corruption (navigate + screenshot base64 intact)

### Yang MASUK POC

- Extension MV3 minimal: connect WS, attach debugger, relay CDP
- Gateway: `/json/version`, `/json`, `/json/new`, WS `/devtools/page/{id}`
- Playwright test: connect → new_page → goto → screenshot
- 1 tab cukup
- Network: direct mode (LAN/WiFi), skip Tailscale

### Yang TIDAK masuk POC

- ❌ Tailscale (direct mode dulu)
- ❌ Auth/password (hardcode skip auth)
- ❌ Snapshot/AXTree builder
- ❌ Multi-tab management
- ❌ Reconnect / state recovery
- ❌ Block detector / fallback chain
- ❌ Integrasi ke Hermes toolset

---

### POC Day 1 — Extension MVP

**Target:** Buktikan `chrome.debugger` jalan di Android

**Deliverable:**
- `manifest.json` dengan `debugger` + `tabs` permission
- `background.js`:
  - `chrome.debugger.attach({tabId}, "1.3")`
  - `chrome.debugger.sendCommand({tabId}, "Page.navigate", {url})`
  - `chrome.debugger.sendCommand({tabId}, "Page.captureScreenshot")`
- Manual test: load extension di Kiwi → trigger navigate → verify screenshot

**Test command (manual):**
```javascript
// Di extension console
chrome.tabs.create({url: "about:blank"}, (tab) => {
  chrome.debugger.attach({tabId: tab.id}, "1.3", () => {
    chrome.debugger.sendCommand({tabId: tab.id}, "Page.navigate", {url: "https://example.com"}, (result) => {
      console.log("Navigate result:", result);
      chrome.debugger.sendCommand({tabId: tab.id}, "Page.captureScreenshot", {format: "jpeg", quality: 60}, (screenshot) => {
        console.log("Screenshot length:", screenshot.data.length);
      });
    });
  });
});
```

**Pass criteria:** Navigate jalan, screenshot base64 valid (bisa decode ke JPEG)

---

### POC Day 2 — Gateway WS Server

**Target:** Emulasi Chrome DevTools `/json` endpoint + WS relay

**Deliverable:**
- `gateway/server.py`:
  - `GET /json/version` → `{Browser: "Chrome/130", "webSocketDebuggerUrl": "ws://..."}`
  - `GET /json` → list tabs: `[{id, type, url, title, webSocketDebuggerUrl}]`
  - `GET /json/new?{url}` → create new tab via extension
  - `WS /devtools/page/{id}` → bidirectional CDP relay
- `gateway/cdp_relay.py`:
  - Extension connect sebagai "backend"
  - Playwright connect sebagai "client"
  - Relay: client → server → extension → CDP → extension → server → client

**Relay architecture:**
```
Playwright (client)                    Extension (backend)
     │                                        │
     │── WS connect /devtools/page/abc ──────│
     │                                        │
     │── {id:1, method:"Page.navigate"} ────▶│
     │                                        │── chrome.debugger.sendCommand
     │                                        │
     │◀── {id:1, result:{...}} ──────────────│
     │                                        │
     │── {id:2, method:"Page.captureScreenshot"} ──▶│
     │                                        │── chrome.debugger.sendCommand
     │                                        │
     │◀── {id:2, result:{data:"base64..."}} ─│
     │                                        │
     │◀── {method:"Page.frameNavigated"} ────│  (CDP events)
     │                                        │
```

**Pass criteria:** Extension connect ke gateway, gateway `/json` respond valid, WS relay jalan

---

### POC Day 3 — Playwright Connect

**Target:** Full end-to-end test

**Deliverable:**
- `tests/test_playwright.py`:
  ```python
  import asyncio
  from playwright.async_api import async_playwright

  async def main():
      async with async_playwright() as p:
          browser = await p.chromium.connect_over_cdp("http://GATEWAY_IP:8765")
          print("Connected:", browser)
          page = await browser.new_page()
          await page.goto("https://example.com")
          await page.screenshot(path="poc_screenshot.png")
          print("Screenshot saved!")
          await page.close()
          browser.close()

  asyncio.run(main())
  ```
- Run di Hermes server (laptop/VPS)
- Extension jalan di HP (same WiFi/LAN)

**Pass criteria:**
1. Playwright connect tanpa error
2. `browser.new_page()` → extension buka tab baru
3. `page.goto()` → halaman ke-load
4. `page.screenshot()` → file tersimpan di server, isinya valid JPEG/PNG
5. **Kalau ini jalan → POC LOLUS**

---

### POC Day 4 — Bug Fix & Edge Cases

**Kemungkinan issue:**
- WS message ordering (CDP command vs event interleaving)
- Binary relay (screenshot base64 gede → WS frame size limit)
- Timing: Playwright expect `Target.targetCreated` event sebelum command lain
- `chrome.debugger` banner di Android (cosmetic, tapi confirm)
- Extension service worker killed di background → reconnect

---

### POC Day 5 — Evaluasi

**Decision matrix:**

| Outcome | Action |
|---|---|
| Semua 3 asumsi terbukti | Lanjut Fase 2: full implementation |
| 1 asumsi gagal | Assess workaround, mungkin pivot |
| 2+ asumsi gagal | Drop project atau rethink arsitektur |

---

## Fase 2 — Full Implementation (2-3 minggu)

*Setelah POC lolos, detail menyusul. Outline:*

1. **Auth & Security** — password auth, Tailscale integration, TLS option
2. **Multi-tab** — tab pool, `browser.newPage()` → `chrome.tabs.create` mapping
3. **Snapshot/AXTree** — `Accessibility.getFullAXTree` → compact format
4. **Reconnect** — WS reconnect, state recovery, keep-alive
5. **Hermes Integration** — backend baru di `tools/environments/`, toolset routing
6. **Block detector** — auto-fallback Browserbase → Extension
7. **Packaging** — extension .crx, gateway pip install

---

## Browser Support

| Browser | Extension | `chrome.debugger` | Status |
|---|---|---|---|
| Kiwi Browser | ✅ MV3 | ✅ | POC target |
| Mises Browser | ✅ MV3 | ✅ | POC target |
| Firefox Android | ⚠️ MV3 partial | ❌ | Tidak didukung |
| Chrome Android | ❌ No extension | N/A | Tidak didukung |

## License

MIT
