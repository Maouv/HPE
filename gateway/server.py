"""
HPE Gateway — FastAPI server

Emulasi Chrome DevTools Protocol endpoint (/json) + WebSocket relay.

Flow:
  Playwright → connect_over_cdp("http://gateway:8765")
    → GET /json/version (browser info)
    → GET /json (tab list)
    → GET /json/new?url=... (create tab)
    → WS /devtools/page/{tabId} (CDP session)

  Extension → WS connect ke ws://gateway:8765/ws (backend channel)
    → Terima CDP commands dari gateway
    → Eksekusi via chrome.debugger
    → Return result ke gateway
    → Forward CDP events

Gateway = middleman:
  Playwright (client) ←→ Gateway ←→ Extension (backend)
"""

import asyncio
import json
import logging
import time
import uuid
from typing import Any, Dict, Optional

import uvicorn
from fastapi import FastAPI, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse

# ─── Logging ─────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
)
log = logging.getLogger('hpe.gateway')

# ─── State ───────────────────────────────────────────────────────────────────

class GatewayState:
    def __init__(self):
        # Extension backend connection
        self.extension_ws: Optional[WebSocket] = None
        self.extension_connected = asyncio.Event()

        # Pending CDP commands: request_id → Future
        self.pending_cdp: Dict[int, asyncio.Future] = {}

        # Session mapping: sessionId → {tabId, ws (playwright client)}
        self.sessions: Dict[str, dict] = {}

        # Reverse: tabId → set of sessionIds
        self.tab_sessions: Dict[int, set] = {}

        # Pending tab creation: request_id → Future
        self.pending_create_tab: Dict[int, asyncio.Future] = {}

        # Pending other requests: request_id → Future
        self.pending_requests: Dict[int, asyncio.Future] = {}

        # Tab list from extension
        self.tabs: list[dict] = []

        # CDP event listeners (per tab): tabId → list of asyncio.Queue
        self.cdp_event_queues: Dict[int, list[asyncio.Queue]] = {}

        # Playwright CDP sessions: tabId → set of client websockets
        self.cdp_clients: Dict[int, set[WebSocket]] = {}

        # tabId → real Chromium main-frame id (from Page.getFrameTree).
        # CRITICAL: Playwright's CRPage keys its internal session map by
        # `targetId` (from Target.attachedToTarget) but looks up sessions by
        # `frame._id` (from Page.getFrameTree) when routing Page.navigate.
        # Real Chrome guarantees targetId === main frame id; our bridge must
        # replicate that by using the REAL frame id as targetId everywhere,
        # not the internal chrome tab id. Otherwise every goto() fails
        # instantly with "Frame has been detached" (session lookup by the
        # real frame id never finds an entry, since it was registered under
        # the tab id instead). See get_real_frame_id().
        self.tab_frame_ids: Dict[int, str] = {}
        self.frame_id_to_tab: Dict[str, int] = {}

        # Request ID counter
        self._next_id = 1

    def next_id(self) -> int:
        rid = self._next_id
        self._next_id += 1
        return rid


state = GatewayState()


async def get_real_frame_id(tab_id: int, timeout: float = 10.0) -> str:
    """Resolve the REAL Chromium main-frame id for a tab via Page.getFrameTree.

    Falls back to str(tab_id) on failure (logged loudly) — that reproduces
    the original bug in the degraded case, but keeps the gateway from hard
    crashing if a tab is somehow unreachable.
    """
    cached = state.tab_frame_ids.get(tab_id)
    if cached:
        return cached

    if not state.extension_ws:
        log.warning(f'[FrameID] no extension_ws, falling back to tab_id for tab={tab_id}')
        return str(tab_id)

    rid = state.next_id()
    future = asyncio.get_event_loop().create_future()
    state.pending_cdp[rid] = future
    try:
        await state.extension_ws.send_json({
            'type': 'cdp_command',
            'id': rid,
            'method': 'Page.getFrameTree',
            'params': {},
            'tabId': tab_id,
        })
        result = await asyncio.wait_for(future, timeout=timeout)
        frame_id = result['frameTree']['frame']['id']
        state.tab_frame_ids[tab_id] = frame_id
        state.frame_id_to_tab[frame_id] = tab_id
        log.info(f'[FrameID] resolved real frame id for tab={tab_id}: {frame_id}')
        return frame_id
    except Exception as e:
        state.pending_cdp.pop(rid, None)
        log.warning(f'[FrameID] failed to resolve real frame id for tab={tab_id}: {e} — falling back to tab_id (this WILL reproduce "Frame has been detached")')
        return str(tab_id)


# ─── FastAPI App ─────────────────────────────────────────────────────────────

app = FastAPI(title='HPE Gateway', version='0.1.0')

# ─── Chrome DevTools /json Emulation ─────────────────────────────────────────

@app.get('/json/version')
async def json_version(request: Request):
    """Emulasi Chrome /json/version endpoint."""
    host = request.headers.get('host', f'localhost:8765')
    ws_url = f'ws://{host}/devtools/browser'
    return JSONResponse({
        'Browser': 'Chrome/130.0.6723.58',
        'Protocol-Version': '1.3',
        'User-Agent': 'HPE-Bridge/0.1.0',
        'V8-Version': '13.0.245.16',
        'WebKit-Version': '537.36 (KHTML, like Gecko)',
        'webSocketDebuggerUrl': ws_url,
        'Target-Type': 'browser',
    })


@app.get('/json')
@app.get('/json/list')
async def json_list(request: Request):
    """Emulasi Chrome /json endpoint — list debuggable targets (tabs)."""
    host = request.headers.get('host', f'localhost:8765')
    targets = []
    for tab in state.tabs:
        tab_url = tab.get('url', '')
        # Skip non-debuggable tabs
        if tab_url.startswith(('content://', 'chrome://', 'chrome-extension://', 'devtools://')):
            continue
        tid = str(tab['id'])
        targets.append({
            'description': '',
            'devtoolsFrontendUrl': f'devtools://devtools/bundled/inspector.html?ws={host}/devtools/page/{tid}',
            'id': tid,
            'title': tab.get('title', ''),
            'type': 'page',
            'url': tab.get('url', ''),
            'webSocketDebuggerUrl': f'ws://{host}/devtools/page/{tid}',
            'parentId': '',
            'browserId': 'HPE-Bridge',
        })
    return JSONResponse(targets)


@app.get('/json/new')
async def json_new(request: Request, url: str = Query('about:blank')):
    """Create new tab — Playwright's browser.newPage() calls this."""
    host = request.headers.get('host', f'localhost:8765')

    if not state.extension_ws:
        return JSONResponse({'error': 'Extension not connected'}, status_code=503)

    rid = state.next_id()
    future = asyncio.get_event_loop().create_future()
    state.pending_create_tab[rid] = future

    # Ask extension to create tab
    await state.extension_ws.send_json({
        'type': 'create_tab',
        'id': rid,
        'url': url,
    })

    try:
        result = await asyncio.wait_for(future, timeout=10.0)
        tab = result['tab']
        tid = str(tab['id'])
        return JSONResponse({
            'description': '',
            'devtoolsFrontendUrl': f'devtools://devtools/bundled/inspector.html?ws={host}/devtools/page/{tid}',
            'id': tid,
            'title': tab.get('title', ''),
            'type': 'page',
            'url': tab.get('url', url),
            'webSocketDebuggerUrl': f'ws://{host}/devtools/page/{tid}',
        })
    except asyncio.TimeoutError:
        return JSONResponse({'error': 'Tab creation timeout'}, status_code=504)


@app.get('/json/close/{target_id}')
async def json_close(target_id: str):
    """Close tab."""
    if not state.extension_ws:
        return JSONResponse({'error': 'Extension not connected'}, status_code=503)

    try:
        tab_id = int(target_id)
    except ValueError:
        return JSONResponse({'error': 'Invalid target ID'}, status_code=400)

    rid = state.next_id()
    future = asyncio.get_event_loop().create_future()
    state.pending_requests[rid] = future

    await state.extension_ws.send_json({
        'type': 'close_tab',
        'id': rid,
        'tabId': tab_id,
    })

    try:
        result = await asyncio.wait_for(future, timeout=5.0)
        return JSONResponse({'success': True})
    except asyncio.TimeoutError:
        return JSONResponse({'error': 'Close timeout'}, status_code=504)


# ─── WebSocket: Extension Backend Channel ────────────────────────────────────

@app.websocket('/ws')
async def extension_websocket(ws: WebSocket):
    """
    Extension connects here. This is the 'backend' channel.
    Extension sends auth, receives CDP commands, sends results/events.
    """
    await ws.accept()
    log.info('Extension connected')

    # Check if already have an extension
    if state.extension_ws is not None:
        log.warning('Another extension already connected, replacing')
        try:
            await state.extension_ws.close()
        except Exception:
            pass

    state.extension_ws = ws
    state.extension_connected.set()

    try:
        while True:
            raw = await ws.receive_text()
            msg = json.loads(raw)
            await handle_extension_message(msg)

    except WebSocketDisconnect:
        log.info('Extension disconnected')
    except Exception as e:
        log.error(f'Extension WS error: {e}')
    finally:
        state.extension_ws = None
        state.extension_connected.clear()
        # Fail all pending requests
        for f in state.pending_cdp.values():
            if not f.done():
                f.set_exception(ConnectionError('Extension disconnected'))
        state.pending_cdp.clear()
        for f in state.pending_create_tab.values():
            if not f.done():
                f.set_exception(ConnectionError('Extension disconnected'))
        state.pending_create_tab.clear()
        for f in state.pending_requests.values():
            if not f.done():
                f.set_exception(ConnectionError('Extension disconnected'))
        state.pending_requests.clear()


async def handle_extension_message(msg: dict):
    """Handle messages from extension."""
    msg_type = msg.get('type')

    if msg_type == 'auth':
        # Extension sends auth — accept for now (no password check in POC)
        if msg.get('password'):
            log.info('Extension auth with password')
        else:
            log.info('Extension auth (no password)')
        await state.extension_ws.send_json({'type': 'auth_ok'})
        # Request tab list
        await state.extension_ws.send_json({'type': 'get_tabs'})
        return

    if msg_type == 'tab_list':
        state.tabs = msg.get('tabs', [])
        log.info(f'Received tab list: {len(state.tabs)} tabs')
        for t in state.tabs:
            log.info(f'  tab: id={t.get("id")} url={t.get("url","")[:80]}')
        return

    if msg_type == 'create_tab_response':
        rid = msg.get('id')
        future = state.pending_create_tab.pop(rid, None)
        if future and not future.done():
            if msg.get('error'):
                future.set_exception(Exception(msg['error']))
            else:
                future.set_result(msg)
            # Add to tabs list
            if msg.get('tab'):
                state.tabs.append(msg['tab'])
        return

    if msg_type == 'close_tab_response':
        rid = msg.get('id')
        future = state.pending_requests.pop(rid, None)
        if future and not future.done():
            if msg.get('error'):
                future.set_exception(Exception(msg['error']))
            else:
                future.set_result(msg)
            # Remove from tabs list
            # (tab_removed event will handle this)
        return

    if msg_type == 'attach_tab_response':
        rid = msg.get('id')
        future = state.pending_requests.pop(rid, None)
        if future and not future.done():
            if msg.get('error'):
                future.set_exception(Exception(msg['error']))
            else:
                future.set_result(msg)
        return

    if msg_type == 'cdp_response':
        rid = msg.get('id')
        err = msg.get('error')
        has_result = msg.get('result') is not None
        log.info(f'CDP response: id={rid} error={err} has_result={has_result} result_keys={list(msg.get("result", {}).keys()) if has_result else []}')
        future = state.pending_cdp.pop(rid, None)
        if future and not future.done():
            if err:
                future.set_exception(Exception(err))
            else:
                future.set_result(msg.get('result'))
        else:
            log.warning(f'CDP response for unknown/expired id={rid}')
        return

    if msg_type == 'cdp_event':
        # Forward CDP event to all Playwright clients for this tab
        tab_id = msg.get('tabId')
        method = msg.get('method')
        params = msg.get('params', {})
        if method in ('Page.frameDetached', 'Page.frameNavigated', 'Page.frameAttached',
                      'Page.navigatedWithinDocument', 'Inspector.detached',
                      'Inspector.targetCrashed'):
            # These are the events that decide whether Playwright thinks the
            # frame/page died mid-navigation. Log params in full — for
            # Page.frameDetached specifically, the 'reason' field ('swap' vs
            # anything else) is the difference between Playwright tolerating
            # it and Playwright throwing "Frame has been detached."
            log.info(f'[EVENT-DETAIL] tab={tab_id} method={method} params={json.dumps(params)}')
        else:
            log.info(f'cdp_event RECEIVED from extension: tab={tab_id} method={method}')
        await broadcast_cdp_event(tab_id, method, params)
        return

    if msg_type == 'tab_updated':
        tab_id = msg.get('tabId')
        for tab in state.tabs:
            if tab['id'] == tab_id:
                tab['url'] = msg.get('url', tab.get('url', ''))
                tab['title'] = msg.get('title', tab.get('title', ''))
                break
        return

    if msg_type == 'tab_removed':
        tab_id = msg.get('tabId')
        state.tabs = [t for t in state.tabs if t['id'] != tab_id]
        return

    if msg_type == 'debugger_detached':
        log.warning(f'Debugger detached: tab={msg.get("tabId")}, reason={msg.get("reason")}')
        return

    if msg_type == 'pong':
        return

    log.warning(f'Unknown message from extension: {msg_type}')


# ─── WebSocket: Playwright CDP Client Channel ────────────────────────────────

@app.websocket('/devtools/page/{tab_id}')
async def cdp_client_websocket(ws: WebSocket, tab_id: int):
    """
    Playwright connects here for CDP session.
    This is the 'client' channel — emulates Chrome's /devtools/page/{id} endpoint.
    """
    await ws.accept()
    log.info(f'CDP client connected for tab {tab_id}')

    # Ensure extension is attached to this tab
    if state.extension_ws:
        rid = state.next_id()
        future = asyncio.get_event_loop().create_future()
        state.pending_requests[rid] = future

        await state.extension_ws.send_json({
            'type': 'attach_tab',
            'id': rid,
            'tabId': tab_id,
        })

        try:
            await asyncio.wait_for(future, timeout=5.0)
            log.info(f'Tab {tab_id} attached')
        except asyncio.TimeoutError:
            log.warning(f'Tab {tab_id} attach timeout')
            await ws.close()
            return
    else:
        log.error('No extension connected')
        await ws.close()
        return

    # Register CDP client for this tab
    if tab_id not in state.cdp_clients:
        state.cdp_clients[tab_id] = set()
    state.cdp_clients[tab_id].add(ws)

    try:
        while True:
            raw = await ws.receive_text()
            msg = json.loads(raw)

            # Playwright CDP message: {id, method, params}
            cdp_id = msg.get('id')
            method = msg.get('method')
            params = msg.get('params', {})

            if not method:
                continue

            # Forward to extension as CDP command
            rid = state.next_id()
            future = asyncio.get_event_loop().create_future()
            state.pending_cdp[rid] = future

            await state.extension_ws.send_json({
                'type': 'cdp_command',
                'id': rid,
                'method': method,
                'params': params,
                'tabId': tab_id,
            })

            # Wait for response
            try:
                result = await asyncio.wait_for(future, timeout=30.0)
                # Send CDP response back to Playwright
                response = {
                    'id': cdp_id,
                    'result': result if result is not None else {},
                }
                await ws.send_text(json.dumps(response))
            except asyncio.TimeoutError:
                error_response = {
                    'id': cdp_id,
                    'error': {'message': 'CDP command timeout'},
                }
                await ws.send_text(json.dumps(error_response))
            except Exception as e:
                error_response = {
                    'id': cdp_id,
                    'error': {'message': str(e)},
                }
                await ws.send_text(json.dumps(error_response))

    except WebSocketDisconnect:
        log.info(f'CDP client disconnected for tab {tab_id}')
    except Exception as e:
        log.error(f'CDP client WS error: {e}')
    finally:
        if tab_id in state.cdp_clients:
            state.cdp_clients[tab_id].discard(ws)
            if not state.cdp_clients[tab_id]:
                del state.cdp_clients[tab_id]


# ─── WebSocket: Browser-level CDP ────────────────────────────────────────────

@app.websocket('/devtools/browser')
async def browser_cdp_websocket(ws: WebSocket):
    """
    Browser-level CDP endpoint.
    Handles Playwright connect_over_cdp flow with session routing.
    """
    await ws.accept()
    log.info('Browser CDP client connected')

    # Request fresh tab list from extension before sending targets
    if state.extension_ws:
        await state.extension_ws.send_json({'type': 'get_tabs'})
        # Wait briefly for fresh tab list
        await asyncio.sleep(0.3)

    # On connect: send targetCreated events for all existing tabs
    # Skip tabs with non-debuggable URLs — extension can't access them
    SKIP_PREFIXES = ('content://', 'chrome://', 'chrome-extension://', 'devtools://')
    for tab in state.tabs:
        tab_url = tab.get('url', '')
        if tab_url.startswith(SKIP_PREFIXES):
            continue
        real_id = await get_real_frame_id(tab['id'])
        await ws.send_text(json.dumps({
            'method': 'Target.targetCreated',
            'params': {
                'targetInfo': {
                    'targetId': real_id,
                    'type': 'page',
                    'title': tab.get('title', ''),
                    'url': tab.get('url', ''),
                    'attached': False,
                    'browserId': 'HPE-Bridge',
                    'browserContextId': 'default',
                }
            }
        }))

    try:
        while True:
            raw = await ws.receive_text()
            msg = json.loads(raw)

            cdp_id = msg.get('id')
            method = msg.get('method')
            params = msg.get('params', {})
            session_id = msg.get('sessionId')

            log.info(f'CDP msg: method={method} id={cdp_id} session={session_id}')

            # ── Session-routed commands ──
            # If message has sessionId, forward to extension as CDP command
            if session_id and session_id in state.sessions:
                sess = state.sessions[session_id]
                tab_id = sess['tabId']

                # Short-circuit: respond to noop-ish commands without hitting extension
                if method in ('Target.setAutoAttach', 'Target.setDiscoverTargets',
                               'Log.enable', 'Log.disable', 'Log.startViolationsReport',
                               'Log.stopViolationsReport'):
                    await ws.send_text(json.dumps({
                        'id': cdp_id,
                        'sessionId': session_id,
                        'result': {},
                    }))
                    continue

                rid = state.next_id()
                future = asyncio.get_event_loop().create_future()
                state.pending_cdp[rid] = future

                if state.extension_ws:
                    log.info(f'Fwd CDP→ext: id={rid} method={method} tab={tab_id} session={session_id}')
                    await state.extension_ws.send_json({
                        'type': 'cdp_command',
                        'id': rid,
                        'method': method,
                        'params': params,
                        'tabId': tab_id,
                    })
                    try:
                        result = await asyncio.wait_for(future, timeout=30.0)
                        # Log key responses for debugging
                        if method in ('Page.getFrameTree', 'Runtime.enable'):
                            log.info(f'CDP response detail: method={method} result={json.dumps(result)[:500]}')

                        # Capture frame info from getFrameTree response
                        if method == 'Page.getFrameTree' and result:
                            frame = result.get('frameTree', {}).get('frame', {})
                            if frame.get('id'):
                                sess['frame_info'] = {
                                    'id': frame['id'],
                                    'url': frame.get('url', ''),
                                    'loaderId': frame.get('loaderId', '1'),
                                }
                                log.info(f'Frame info captured: id={frame["id"]} url={frame.get("url","")[:80]}')

                        await ws.send_text(json.dumps({
                            'id': cdp_id,
                            'sessionId': session_id,
                            'result': result if result is not None else {},
                        }))

                        # ── Synthetic events — safety net for Android ──
                        # Wake-up Runtime.enable in attachDebugger() primes the
                        # onEvent pipeline, but initial events fire BEFORE
                        # Playwright connects → lost. These synthetic events
                        # ensure Playwright completes init.
                        # If real onEvent events also arrive → Playwright handles
                        # idempotently (duplicate contextId → update, not crash).

                        if method == 'Runtime.enable':
                            fi = sess.get('frame_info', {})
                            fid = fi.get('id', '1')
                            origin = fi.get('url', 'about:blank')
                            sess['ctx_counter'] = 1  # track fake context ids per session
                            await asyncio.sleep(0.05)
                            await ws.send_text(json.dumps({
                                'method': 'Runtime.executionContextCreated',
                                'params': {
                                    'context': {
                                        'id': 1,
                                        'origin': origin,
                                        'name': '',
                                        'auxData': {
                                            'frameId': fid,
                                            'isDefault': True,
                                        },
                                    }
                                },
                                'sessionId': session_id,
                            }))
                            log.info(f'[SYNTH] executionContextCreated (init): frame={fid} ctx=1')

                        elif method == 'Page.setLifecycleEventsEnabled':
                            fi = sess.get('frame_info', {})
                            fid = fi.get('id', '1')
                            lid = fi.get('loaderId', '1')
                            ts = time.time()
                            await asyncio.sleep(0.05)
                            for evt in ('DOMContentLoaded', 'load'):
                                await ws.send_text(json.dumps({
                                    'method': 'Page.lifecycleEvent',
                                    'params': {
                                        'frameId': fid,
                                        'loaderId': lid,
                                        'name': evt,
                                        'timestamp': ts,
                                    },
                                    'sessionId': session_id,
                                }))
                            log.info(f'[SYNTH] lifecycleEvent (init): frame={fid} loader={lid}')

                        # ── NEW: hook real navigations, not just init ──
                        # Without this, page.goto() called *after* connect (i.e. every
                        # actual use of this tool, see test_goto.py step 5) still hangs:
                        # the init hooks above only fire once, at attach time. A real
                        # Page.navigate has its own new loaderId/frameId and Playwright
                        # waits on lifecycle/frameNavigated events keyed to THAT loaderId,
                        # which nothing was synthesizing before this patch.
                        #
                        # CAVEAT (unresolved): executionContextId below is still a fake,
                        # locally-incrementing counter, NOT a real V8 context id from
                        # Chromium. If Runtime.evaluate/callFunctionOn is later sent by
                        # Playwright using this fake id, chrome.debugger.sendCommand will
                        # forward it verbatim to the real backend, which will very likely
                        # reject it ("Cannot find context with specified id"). This patch
                        # does NOT fix page.evaluate() / set_content() (test_goto.py steps
                        # 7-10) — only unblocks goto()'s navigation-wait. Expect step 5 to
                        # pass and steps 7/9 to still fail unless real onEvent turns out to
                        # fire after all (check logs for "cdp_event RECEIVED from extension"
                        # — if that line NEVER appears during this run, onEvent is
                        # confirmed dead on this platform, not just slow).
                        elif method == 'Page.navigate' and result:
                            fid = result.get('frameId') or sess.get('frame_info', {}).get('id', '1')
                            lid = result.get('loaderId', '1')
                            nav_url = params.get('url', '')
                            nav_error = result.get('errorText')

                            sess['frame_info'] = {'id': fid, 'url': nav_url, 'loaderId': lid}
                            sess['ctx_counter'] = sess.get('ctx_counter', 1) + 1
                            new_ctx_id = sess['ctx_counter']
                            ts = time.time()

                            if nav_error:
                                # Navigation itself failed at the CDP level — don't
                                # synthesize a fake success, that would desync Playwright
                                # further. Let the real error surface via the command result.
                                log.warning(f'[SYNTH] Page.navigate returned errorText={nav_error}, skipping synthetic events for frame={fid}')
                            else:
                                await asyncio.sleep(0.05)
                                await ws.send_text(json.dumps({
                                    'method': 'Page.frameNavigated',
                                    'params': {
                                        'frame': {
                                            'id': fid,
                                            'loaderId': lid,
                                            'url': nav_url,
                                            'securityOrigin': nav_url,
                                            'mimeType': 'text/html',
                                        },
                                        'type': 'Navigation',
                                    },
                                    'sessionId': session_id,
                                }))
                                await ws.send_text(json.dumps({
                                    'method': 'Runtime.executionContextCreated',
                                    'params': {
                                        'context': {
                                            'id': new_ctx_id,
                                            'origin': nav_url,
                                            'name': '',
                                            'auxData': {
                                                'frameId': fid,
                                                'isDefault': True,
                                            },
                                        }
                                    },
                                    'sessionId': session_id,
                                }))
                                for evt in ('DOMContentLoaded', 'load'):
                                    await ws.send_text(json.dumps({
                                        'method': 'Page.lifecycleEvent',
                                        'params': {
                                            'frameId': fid,
                                            'loaderId': lid,
                                            'name': evt,
                                            'timestamp': ts,
                                        },
                                        'sessionId': session_id,
                                    }))
                                log.info(f'[SYNTH] frameNavigated+context+lifecycle for real Page.navigate: frame={fid} loader={lid} ctx={new_ctx_id} url={nav_url[:80]}')

                    except asyncio.TimeoutError:
                        await ws.send_text(json.dumps({
                            'id': cdp_id,
                            'sessionId': session_id,
                            'error': {'message': 'CDP command timeout'},
                        }))
                    except Exception as e:
                        # Tab might be closed — send error, don't crash WS
                        log.warning(f'CDP command failed for tab {tab_id}: {e}')
                        await ws.send_text(json.dumps({
                            'id': cdp_id,
                            'sessionId': session_id,
                            'error': {'message': str(e)},
                        }))
                else:
                    await ws.send_text(json.dumps({
                        'id': cdp_id,
                        'sessionId': session_id,
                        'error': {'message': 'Extension not connected'},
                    }))
                continue

            if not method:
                continue

            # ── Browser-level methods ──

            if method == 'Browser.getVersion':
                await ws.send_text(json.dumps({
                    'id': cdp_id,
                    'result': {
                        'protocolVersion': '1.3',
                        'product': 'Chrome/130.0.6723.58',
                        'revision': '@abcdef',
                        'userAgent': 'HPE-Bridge/0.1.0',
                        'jsVersion': '13.0.245.16',
                    },
                }))

            elif method == 'Target.createBrowserContext':
                await ws.send_text(json.dumps({
                    'id': cdp_id,
                    'result': {'browserContextId': 'default'},
                }))

            elif method == 'Target.disposeBrowserContext':
                await ws.send_text(json.dumps({
                    'id': cdp_id,
                    'result': {},
                }))

            elif method == 'Target.setDiscoverTargets':
                await ws.send_text(json.dumps({
                    'id': cdp_id,
                    'result': {},
                }))

            elif method == 'Target.setAutoAttach':
                # Playwright sends this at TWO levels:
                #   1. Browser-level (session_id is None) — "discover top-level pages"
                #   2. Page-level (session_id is a real sessionId) — "discover sub-targets
                #      (OOPIF/worker) INSIDE that specific page session"
                #
                # BUG (fixed here): the old code treated every call identically and
                # re-looped over ALL tabs + minted a brand-new sessionId + re-sent
                # Target.attachedToTarget EVERY time, regardless of which level the
                # call came from. Since Playwright sends this 1x browser-level + 1x
                # per attached page, a session with N pages got N+1 total calls, and
                # each pre-existing tab ended up with N+1 *different* sessionIds all
                # live in tab_sessions[tab_id] simultaneously.
                #
                # Consequences that were observed:
                #   - broadcast_cdp_event() fans a single real CDP event out to every
                #     sessionId in tab_sessions[tab_id] → one real event got sent
                #     N+1 times (this is why executionContextCreated appeared ~10x —
                #     it's fan-out from duplicate sessions, not Chromium re-firing).
                #   - Playwright, seeing a SECOND Target.attachedToTarget for a
                #     targetId it already has an active session for, treats the
                #     earlier session's frame as superseded → "Frame has been
                #     detached" on the next navigation.
                #
                # Fix: only mint a new session for a tab that doesn't already have
                # one. Page-level calls (session_id is not None) don't re-declare
                # top-level tabs at all — this bridge has no OOPIF/worker discovery
                # to offer, so just ack.
                if session_id is None:
                    SKIP_PREFIXES = ('content://', 'chrome://', 'chrome-extension://', 'devtools://')
                    for tab in state.tabs:
                        tab_url = tab.get('url', '')
                        if tab_url.startswith(SKIP_PREFIXES):
                            continue
                        tab_id = tab['id']

                        if state.tab_sessions.get(tab_id):
                            log.info(f'[AutoAttach] tab={tab_id} already has session(s) {state.tab_sessions[tab_id]} — skipping duplicate attach')
                            continue

                        sid = f'session-{tab_id}-{state.next_id()}'
                        state.sessions[sid] = {'tabId': tab_id, 'ws': ws}
                        state.tab_sessions.setdefault(tab_id, set()).add(sid)

                        # CRITICAL: targetId must be the REAL Chromium frame id,
                        # not our internal tab_id. See get_real_frame_id() docstring —
                        # CRPage keys its session map by this targetId, but looks
                        # sessions up later by frame._id when routing Page.navigate.
                        # If they don't match, goto() throws "Frame has been
                        # detached." instantly, every single time, with zero wire
                        # traffic (this was the actual root cause behind that error
                        # in every prior test run, independent of the duplicate-
                        # session bug fixed above).
                        real_id = await get_real_frame_id(tab_id)

                        await ws.send_text(json.dumps({
                            'method': 'Target.attachedToTarget',
                            'params': {
                                'sessionId': sid,
                                'targetInfo': {
                                    'targetId': real_id,
                                    'type': 'page',
                                    'title': tab.get('title', ''),
                                    'url': tab.get('url', ''),
                                    'attached': True,
                                    'browserId': 'HPE-Bridge',
                                    'browserContextId': 'default',
                                },
                            }
                        }))
                        log.info(f'[AutoAttach] new session={sid} for tab={tab_id} (browser-level)')
                else:
                    # Page-level auto-attach for sub-targets — nothing to discover.
                    log.info(f'[AutoAttach] page-level call in session={session_id}, no sub-targets to attach — ack only')

                # Acknowledge
                await ws.send_text(json.dumps({
                    'id': cdp_id,
                    'result': {},
                }))

            elif method == 'Browser.setDownloadBehavior':
                await ws.send_text(json.dumps({
                    'id': cdp_id,
                    'result': {},
                }))

            elif method == 'Target.getTargetInfo':
                await ws.send_text(json.dumps({
                    'id': cdp_id,
                    'result': {
                        'targetInfo': {
                            'targetId': 'HPE-Bridge',
                            'type': 'browser',
                            'title': '',
                            'url': '',
                            'attached': True,
                            'browserId': 'HPE-Bridge',
                            'browserContextId': 'default',
                        }
                    },
                }))
                log.info('Target.getTargetInfo response sent')

            elif method == 'Target.getTargets':
                targets = []
                for tab in state.tabs:
                    real_id = await get_real_frame_id(tab['id'])
                    targets.append({
                        'targetId': real_id,
                        'type': 'page',
                        'title': tab.get('title', ''),
                        'url': tab.get('url', ''),
                        'attached': False,
                        'browserId': 'HPE-Bridge',
                        'browserContextId': 'default',
                    })
                await ws.send_text(json.dumps({
                    'id': cdp_id,
                    'result': {'targetInfos': targets},
                }))

            elif method == 'Target.attachToTarget':
                # Attach to specific target — return sessionId.
                # target_id here is whatever WE previously told Playwright as
                # 'targetId' (the real frame id, per the fix above) — NOT
                # necessarily our internal tab_id. Resolve via the reverse map
                # first; only fall back to treating it as a raw tab_id for
                # backward compat with any internal caller that still passes one.
                target_id = params.get('targetId')
                tid = state.frame_id_to_tab.get(target_id)
                if tid is None:
                    try:
                        tid = int(target_id)
                    except (ValueError, TypeError):
                        tid = target_id
                    log.warning(f'[attachToTarget] targetId={target_id} not found in frame_id_to_tab map, falling back to raw value as tab_id={tid}')

                sid = f'session-{tid}-{state.next_id()}'
                state.sessions[sid] = {'tabId': tid, 'ws': ws}
                if tid not in state.tab_sessions:
                    state.tab_sessions[tid] = set()
                state.tab_sessions[tid].add(sid)

                await ws.send_text(json.dumps({
                    'id': cdp_id,
                    'result': {'sessionId': sid},
                }))

            elif method == 'Target.createTarget':
                # Create new tab
                url = params.get('url', 'about:blank')
                rid = state.next_id()
                future = asyncio.get_event_loop().create_future()
                state.pending_create_tab[rid] = future

                if state.extension_ws:
                    await state.extension_ws.send_json({
                        'type': 'create_tab',
                        'id': rid,
                        'url': url,
                    })
                    try:
                        result = await asyncio.wait_for(future, timeout=10.0)
                        tab = result['tab']
                        tid = tab['id']

                        # Resolve the real frame id for this brand-new tab BEFORE
                        # telling Playwright about it — same fix as setAutoAttach.
                        real_id = await get_real_frame_id(tid)

                        # Send targetCreated event
                        await ws.send_text(json.dumps({
                            'method': 'Target.targetCreated',
                            'params': {
                                'targetInfo': {
                                    'targetId': real_id,
                                    'type': 'page',
                                    'title': tab.get('title', ''),
                                    'url': tab.get('url', url),
                                    'attached': False,
                                    'browserId': 'HPE-Bridge',
                                    'browserContextId': 'default',
                                }
                            }
                        }))

                        # Auto-attach to the new tab
                        sid = f'session-{tid}-{state.next_id()}'
                        state.sessions[sid] = {'tabId': tid, 'ws': ws}
                        if tid not in state.tab_sessions:
                            state.tab_sessions[tid] = set()
                        state.tab_sessions[tid].add(sid)

                        await ws.send_text(json.dumps({
                            'method': 'Target.attachedToTarget',
                            'params': {
                                'sessionId': sid,
                                'targetInfo': {
                                    'targetId': real_id,
                                    'type': 'page',
                                    'title': tab.get('title', ''),
                                    'url': tab.get('url', url),
                                    'attached': True,
                                    'browserId': 'HPE-Bridge',
                                    'browserContextId': 'default',
                                },
                            }
                        }))

                        # Send response
                        await ws.send_text(json.dumps({
                            'id': cdp_id,
                            'result': {'targetId': real_id},
                        }))
                    except asyncio.TimeoutError:
                        await ws.send_text(json.dumps({
                            'id': cdp_id,
                            'error': {'message': 'Tab creation timeout'},
                        }))
                else:
                    await ws.send_text(json.dumps({
                        'id': cdp_id,
                        'error': {'message': 'Extension not connected'},
                    }))

            elif method == 'Target.closeTarget':
                target_id = params.get('targetId')
                tid = state.frame_id_to_tab.get(target_id)
                if tid is None:
                    try:
                        tid = int(target_id)
                    except (ValueError, TypeError):
                        tid = None
                if tid is not None:
                    rid = state.next_id()
                    future = asyncio.get_event_loop().create_future()
                    state.pending_requests[rid] = future

                    if state.extension_ws:
                        await state.extension_ws.send_json({
                            'type': 'close_tab',
                            'id': rid,
                            'tabId': tid,
                        })
                        try:
                            await asyncio.wait_for(future, timeout=5.0)
                        except asyncio.TimeoutError:
                            pass

                    await ws.send_text(json.dumps({
                        'id': cdp_id,
                        'result': {},
                    }))
                else:
                    await ws.send_text(json.dumps({
                        'id': cdp_id,
                        'error': {'message': 'Invalid target ID'},
                    }))

            else:
                # Unknown browser-level method — return empty result
                log.warning(f'Unhandled browser CDP method: {method}')
                await ws.send_text(json.dumps({
                    'id': cdp_id,
                    'result': {},
                }))

    except WebSocketDisconnect:
        log.info('Browser CDP client disconnected')
    except Exception as e:
        log.error(f'Browser CDP WS error: {e}')
    finally:
        # Clean up sessions belonging to this ws
        to_remove = [sid for sid, s in state.sessions.items() if s['ws'] is ws]
        for sid in to_remove:
            sess = state.sessions.pop(sid, None)
            if sess:
                tid = sess['tabId']
                if tid in state.tab_sessions:
                    state.tab_sessions[tid].discard(sid)
                    if not state.tab_sessions[tid]:
                        del state.tab_sessions[tid]


# ─── CDP Event Broadcast ────────────────────────────────────────────────────

async def broadcast_cdp_event(tab_id: int, method: str, params: dict):
    """Broadcast CDP event to Playwright clients via session routing."""
    # Route via sessions
    sids = state.tab_sessions.get(tab_id, set())
    if not sids:
        log.warning(f'broadcast_cdp_event: tab={tab_id} method={method} — NO session mapped, DROPPED. known tab_sessions keys={list(state.tab_sessions.keys())}')
        return
    log.info(f'broadcast_cdp_event: tab={tab_id} method={method} → forwarding to {len(sids)} session(s)')

    dead = []
    for sid in list(sids):
        sess = state.sessions.get(sid)
        if not sess:
            continue
        ws = sess['ws']
        try:
            await ws.send_text(json.dumps({
                'method': method,
                'params': params,
                'sessionId': sid,
            }))
        except Exception:
            dead.append(sid)

    for sid in dead:
        sids.discard(sid)


# ─── Main ────────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    import sys

    host = '0.0.0.0'
    port = 8765

    # Parse args
    if '--host' in sys.argv:
        host = sys.argv[sys.argv.index('--host') + 1]
    if '--port' in sys.argv:
        port = int(sys.argv[sys.argv.index('--port') + 1])

    log.info(f'Starting HPE Gateway on {host}:{port}')
    uvicorn.run(app, host=host, port=port, log_level='info')
