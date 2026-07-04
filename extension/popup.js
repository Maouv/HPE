/**
 * HPE Popup Logic
 * Toggle on/off + save config
 */

document.addEventListener('DOMContentLoaded', async () => {
  const dot = document.getElementById('statusDot');
  const statusText = document.getElementById('statusText');
  const toggle = document.getElementById('toggleBridge');
  const urlInput = document.getElementById('gatewayUrl');
  const passInput = document.getElementById('password');
  const btnSave = document.getElementById('btnSave');

  // Load saved config + status
  chrome.runtime.sendMessage({ type: 'get_status' }, (status) => {
    if (chrome.runtime.lastError || !status) return;
    urlInput.value = status.gatewayUrl || '';
    toggle.checked = status.connected;
    updateUI(status.connected, status.gatewayUrl);
  });

  // Toggle on/off
  toggle.addEventListener('change', () => {
    if (toggle.checked) {
      const url = urlInput.value.trim();
      if (!url) {
        toggle.checked = false;
        return;
      }
      chrome.runtime.sendMessage({
        type: 'connect',
        gatewayUrl: url,
        password: passInput.value.trim(),
      }, () => {
        updateUI(true, url);
      });
    } else {
      chrome.runtime.sendMessage({ type: 'disconnect' }, () => {
        updateUI(false);
      });
    }
  });

  // Save config
  btnSave.addEventListener('click', () => {
    chrome.runtime.sendMessage({
      type: 'save_config',
      gatewayUrl: urlInput.value.trim(),
      password: passInput.value.trim(),
    }, () => {
      btnSave.textContent = 'Saved!';
      setTimeout(() => btnSave.textContent = 'Save Config', 1500);
    });
  });

  // Poll status
  setInterval(() => {
    chrome.runtime.sendMessage({ type: 'get_status' }, (status) => {
      if (chrome.runtime.lastError || !status) return;
      // Sync toggle with actual state
      if (toggle.checked !== status.connected) {
        toggle.checked = status.connected;
        updateUI(status.connected, status.gatewayUrl);
      }
    });
  }, 1500);

  function updateUI(connected, url) {
    if (connected) {
      dot.className = 'dot on';
      statusText.textContent = 'Connected to ' + (url || 'gateway');
    } else {
      dot.className = 'dot off';
      statusText.textContent = 'Off';
    }
  }
});
