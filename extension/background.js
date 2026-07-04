/**
 * HPE Background Service Worker
 *
 * Bridge antara WebSocket (gateway) dan chrome.debugger (CDP).
 * Extension ini jual diri sebagai Chrome remote debugging instance.
 *
 * Flow:
 *   Gateway ←──WS──→ Extension ←──chrome.debugger──→ Tab
 *
 * Extension bertindak sebagai "backend" yang:
 *   1. Connect ke gateway via WebSocket
 *   2. Terima CDP command dari gateway
 *   3. Eksekusi via chrome.debugger.sendCommand
 *   4. Return result ke gateway
 *   5. Forward CDP events (Page.frameNavigated, dll) ke gateway
 */

// ─── State ──────────────────────────────────────────────────────────────────

const STATE = {
  ws: null,              // WebSocket connection ke gateway
  connected: false,      // WS connected?
  gatewayUrl: '',        // ws://ip:port
  password: '',          // optional auth
  attachedTabs: new Map(), // tabId → { url, title, faviconUrl }
  pendingTabs: new Map(),  // tabId → callback (buat /json/new)
  commandCallbacks: new Map(), // id → callback (buat CDP command response)
  reconnectTimer: null,
  reconnectDelay: 1000,  // start 1s, max 30s
};

// ─── Config ─────────────────────────────────────────────────────────────────

const DEFAULT_CONFIG = {
  gatewayUrl: 'ws://192.168.1.100:8765',
  password: '',
  autoConnect: false,
};

// ─── Storage Helpers ────────────────────────────────────────────────────────

async function getConfig() {
  const result = await chrome.storage.local.get('hpe_config');
  return { ...DEFAULT_CONFIG, ...(result.hpe_config || {}) };
}

async function setConfig(updates) {
  const current = await getConfig();
  const next = { ...current, ...updates };
  await chrome.storage.local.set({ hpe_config: next });
  return next;
}

// ─── WebSocket Connection ───────────────────────────────────────────────────

function connect(gatewayUrl, password) {
  if (STATE.ws && (STATE.ws.readyState === WebSocket.OPEN || STATE.ws.readyState === WebSocket.CONNECTING)) {
    console.log('[HPE] Already connected or connecting');
    return;
  }

  console.log('[HPE] Connecting to', gatewayUrl);
  STATE.gatewayUrl = gatewayUrl;
  STATE.password = password || '';

  try {
    const ws = new WebSocket(gatewayUrl);
    STATE.ws = ws;

    ws.onopen = () => {
      console.log('[HPE] WebSocket connected');
      STATE.connected = true;
      STATE.reconnectDelay = 1000; // reset backoff

      // Send auth
      const authMsg = {
        type: 'auth',
        password: STATE.password,
        userAgent: navigator.userAgent,
      };
      ws.send(JSON.stringify(authMsg));
      updateBadge('on');
    };

    ws.onmessage = (event) => {
      handleGatewayMessage(event.data);
    };

    ws.onerror = (error) => {
      console.error('[HPE] WebSocket error:', error);
    };

    ws.onclose = () => {
      console.log('[HPE] WebSocket disconnected');
      STATE.connected = false;
      STATE.ws = null;
      updateBadge('off');
      scheduleReconnect();
    };

  } catch (err) {
    console.error('[HPE] Connect failed:', err);
    scheduleReconnect();
  }
}

function disconnect() {
  if (STATE.reconnectTimer) {
    clearTimeout(STATE.reconnectTimer);
    STATE.reconnectTimer = null;
  }
  if (STATE.ws) {
    STATE.ws.onclose = null; // prevent reconnect
    STATE.ws.close();
    STATE.ws = null;
  }
  STATE.connected = false;
  STATE.reconnectDelay = 1000;
  updateBadge('off');
}

function scheduleReconnect() {
  if (STATE.reconnectTimer) return;
  if (!STATE.gatewayUrl) return; // no gateway configured

  console.log('[HPE] Reconnecting in', STATE.reconnectDelay, 'ms');
  STATE.reconnectTimer = setTimeout(async () => {
    STATE.reconnectTimer = null;
    const config = await getConfig();
    if (config.autoConnect || STATE.gatewayUrl) {
      connect(STATE.gatewayUrl, STATE.password);
    }
  }, STATE.reconnectDelay);

  // Exponential backoff, max 30s
  STATE.reconnectDelay = Math.min(STATE.reconnectDelay * 2, 30000);
}

// ─── Gateway Message Handler ────────────────────────────────────────────────

async function handleGatewayMessage(rawData) {
  let msg;
  try {
    msg = JSON.parse(rawData);
  } catch (e) {
    console.error('[HPE] Invalid JSON from gateway:', e);
    return;
  }

  // Auth response
  if (msg.type === 'auth_ok') {
    console.log('[HPE] Authenticated with gateway');
    // Send current tab list
    await sendTabList();
    return;
  }

  if (msg.type === 'auth_error') {
    console.error('[HPE] Auth failed:', msg.error);
    disconnect();
    return;
  }

  // CDP command from gateway (Playwright → extension)
  if (msg.type === 'cdp_command') {
    await handleCdpCommand(msg);
    return;
  }

  // Tab list request
  if (msg.type === 'get_tabs') {
    await sendTabList();
    return;
  }

  // Create new tab
  if (msg.type === 'create_tab') {
    await handleCreateTab(msg);
    return;
  }

  // Close tab
  if (msg.type === 'close_tab') {
    await handleCloseTab(msg);
    return;
  }

  // Attach to tab
  if (msg.type === 'attach_tab') {
    await handleAttachTab(msg);
    return;
  }

  // Detach from tab
  if (msg.type === 'detach_tab') {
    await handleDetachTab(msg);
    return;
  }

  // Ping
  if (msg.type === 'ping') {
    sendToGateway({ type: 'pong', timestamp: Date.now() });
    return;
  }

  console.warn('[HPE] Unknown message type:', msg.type);
}

// ─── CDP Command Handler ────────────────────────────────────────────────────

async function handleCdpCommand(msg) {
  const { id, method, params, tabId } = msg;
  console.log('[HPE] CDP command:', id, method, 'tab:', tabId);

  try {
    // Ensure tab is attached
    if (!STATE.attachedTabs.has(tabId)) {
      await attachDebugger(tabId);
    }

    // Send CDP command via chrome.debugger
    const result = await new Promise((resolve, reject) => {
      chrome.debugger.sendCommand(
        { tabId: tabId },
        method,
        params || {},
        (result) => {
          if (chrome.runtime.lastError) {
            reject(new Error(chrome.runtime.lastError.message));
          } else {
            resolve(result);
          }
        }
      );
    });

    // Send result back to gateway
    sendToGateway({
      type: 'cdp_response',
      id: id,
      result: result,
      error: null,
    });

  } catch (err) {
    console.error('[HPE] CDP command failed:', id, method, err);
    sendToGateway({
      type: 'cdp_response',
      id: id,
      result: null,
      error: err.message,
    });
  }
}

// ─── Tab Management ─────────────────────────────────────────────────────────

async function handleCreateTab(msg) {
  const { id, url } = msg;
  console.log('[HPE] Creating tab:', url);

  try {
    const tab = await new Promise((resolve, reject) => {
      chrome.tabs.create({ url: url || 'about:blank' }, (tab) => {
        if (chrome.runtime.lastError) {
          reject(new Error(chrome.runtime.lastError.message));
        } else {
          resolve(tab);
        }
      });
    });

    console.log('[HPE] Tab created:', tab.id, tab.url);

    // Send tab info back
    sendToGateway({
      type: 'create_tab_response',
      id: id,
      tab: {
        id: tab.id,
        url: tab.url,
        title: tab.title || '',
        windowId: tab.windowId,
      },
    });

    // Auto-attach debugger to new tab
    await attachDebugger(tab.id);

  } catch (err) {
    console.error('[HPE] Create tab failed:', err);
    sendToGateway({
      type: 'create_tab_response',
      id: id,
      error: err.message,
    });
  }
}

async function handleCloseTab(msg) {
  const { id, tabId } = msg;
  console.log('[HPE] Closing tab:', tabId);

  try {
    await new Promise((resolve, reject) => {
      chrome.tabs.remove(tabId, () => {
        if (chrome.runtime.lastError) {
          reject(new Error(chrome.runtime.lastError.message));
        } else {
          resolve();
        }
      });
    });

    STATE.attachedTabs.delete(tabId);
    sendToGateway({ type: 'close_tab_response', id: id, success: true });
  } catch (err) {
    sendToGateway({ type: 'close_tab_response', id: id, error: err.message });
  }
}

async function handleAttachTab(msg) {
  const { id, tabId } = msg;
  try {
    await attachDebugger(tabId);
    sendToGateway({ type: 'attach_tab_response', id: id, success: true });
  } catch (err) {
    sendToGateway({ type: 'attach_tab_response', id: id, error: err.message });
  }
}

async function handleDetachTab(msg) {
  const { id, tabId } = msg;
  try {
    await detachDebugger(tabId);
    sendToGateway({ type: 'detach_tab_response', id: id, success: true });
  } catch (err) {
    sendToGateway({ type: 'detach_tab_response', id: id, error: err.message });
  }
}

// ─── Debugger (CDP) Bridge ──────────────────────────────────────────────────

async function attachDebugger(tabId) {
  if (STATE.attachedTabs.has(tabId)) {
    console.log('[HPE] Tab already attached:', tabId);
    return;
  }

  console.log('[HPE] Attaching debugger to tab:', tabId);

  await new Promise((resolve, reject) => {
    chrome.debugger.attach({ tabId: tabId }, '1.3', () => {
      if (chrome.runtime.lastError) {
        reject(new Error(chrome.runtime.lastError.message));
      } else {
        STATE.attachedTabs.set(tabId, { url: '', title: '' });
        console.log('[HPE] Debugger attached:', tabId);
        resolve();
      }
    });
  });
}

async function detachDebugger(tabId) {
  if (!STATE.attachedTabs.has(tabId)) {
    return;
  }

  await new Promise((resolve, reject) => {
    chrome.debugger.detach({ tabId: tabId }, () => {
      if (chrome.runtime.lastError) {
        reject(new Error(chrome.runtime.lastError.message));
      } else {
        STATE.attachedTabs.delete(tabId);
        console.log('[HPE] Debugger detached:', tabId);
        resolve();
      }
    });
  });
}

// ─── CDP Event Forwarding ───────────────────────────────────────────────────

chrome.debugger.onEvent.addListener((source, method, params) => {
  // Forward CDP events ke gateway (Page.frameNavigated, Runtime.consoleAPICalled, dll)
  sendToGateway({
    type: 'cdp_event',
    tabId: source.tabId,
    method: method,
    params: params,
  });
});

chrome.debugger.onDetach.addListener((source, reason) => {
  console.log('[HPE] Debugger detached by browser:', source.tabId, reason);
  STATE.attachedTabs.delete(source.tabId);
  sendToGateway({
    type: 'debugger_detached',
    tabId: source.tabId,
    reason: reason,
  });
});

// ─── Tab Events ─────────────────────────────────────────────────────────────

chrome.tabs.onUpdated.addListener(async (tabId, changeInfo, tab) => {
  if (STATE.attachedTabs.has(tabId)) {
    const info = STATE.attachedTabs.get(tabId);
    if (changeInfo.url) info.url = changeInfo.url;
    if (changeInfo.title) info.title = changeInfo.title;
    STATE.attachedTabs.set(tabId, info);

    // Notify gateway
    sendToGateway({
      type: 'tab_updated',
      tabId: tabId,
      url: tab.url,
      title: tab.title,
    });
  }
});

chrome.tabs.onRemoved.addListener((tabId) => {
  if (STATE.attachedTabs.has(tabId)) {
    STATE.attachedTabs.delete(tabId);
    sendToGateway({ type: 'tab_removed', tabId: tabId });
  }
});

// ─── Helpers ────────────────────────────────────────────────────────────────

function sendToGateway(msg) {
  if (STATE.ws && STATE.ws.readyState === WebSocket.OPEN) {
    STATE.ws.send(JSON.stringify(msg));
  }
}

async function sendTabList() {
  const tabs = await new Promise((resolve) => {
    chrome.tabs.query({}, (tabs) => resolve(tabs || []));
  });

  const tabList = tabs
    .filter(t => !t.url.startsWith('chrome://') && !t.url.startsWith('chrome-extension://'))
    .map(t => ({
      id: t.id,
      url: t.url,
      title: t.title || '',
    }));

  sendToGateway({ type: 'tab_list', tabs: tabList });
}

function updateBadge(state) {
  const text = state === 'on' ? 'ON' : '';
  const color = state === 'on' ? '#00aa00' : '#666666';
  chrome.action.setBadgeText({ text });
  chrome.action.setBadgeBackgroundColor({ color });
}

// ─── Message from Popup ─────────────────────────────────────────────────────

chrome.runtime.onMessage.addListener((msg, sender, sendResponse) => {
  if (msg.type === 'get_status') {
    sendResponse({
      connected: STATE.connected,
      gatewayUrl: STATE.gatewayUrl,
      attachedTabs: STATE.attachedTabs.size,
    });
    return true;
  }

  if (msg.type === 'connect') {
    connect(msg.gatewayUrl, msg.password);
    sendResponse({ ok: true });
    return true;
  }

  if (msg.type === 'disconnect') {
    disconnect();
    sendResponse({ ok: true });
    return true;
  }

  if (msg.type === 'save_config') {
    setConfig({ gatewayUrl: msg.gatewayUrl, password: msg.password }).then(() => {
      sendResponse({ ok: true });
    });
    return true;
  }
});

// ─── Auto-connect on startup ────────────────────────────────────────────────

chrome.runtime.onStartup.addListener(async () => {
  const config = await getConfig();
  if (config.autoConnect) {
    connect(config.gatewayUrl, config.password);
  }
});

chrome.runtime.onInstalled.addListener(async () => {
  const config = await getConfig();
  if (config.autoConnect) {
    connect(config.gatewayUrl, config.password);
  }
});

console.log('[HPE] Background service worker loaded');
