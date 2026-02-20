async function loadJson(path) {
  try {
    const response = await fetch(path, { cache: 'no-store' });
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    return await response.json();
  } catch (error) {
    console.error(`Failed to load ${path}`, error);
    return null;
  }
}

function setLastUpdated(id, payload) {
  const el = document.getElementById(id);
  if (!el) return;
  const updated = payload?.updated_at;
  el.textContent = updated ? `Last updated: ${new Date(updated).toLocaleString()}` : 'Last updated: unavailable';
}

function renderNoData(containerId, message = 'No data available.') {
  const el = document.getElementById(containerId);
  if (!el) return;
  el.innerHTML = `<div class="no-data">${message}</div>`;
}
