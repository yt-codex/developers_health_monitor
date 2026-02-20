(async function initNews() {
  const payload = await loadJson('./data/news.json');
  setLastUpdated('news-updated', payload);

  const list = document.getElementById('news-list');
  if (!list) return;

  const items = payload?.items || [];
  if (!items.length) {
    renderNoData('news-list', 'No news items available.');
    return;
  }

  const severityFilter = document.getElementById('severity-filter');
  const textFilter = document.getElementById('text-filter');

  function draw() {
    const sev = severityFilter?.value || 'all';
    const term = (textFilter?.value || '').toLowerCase();

    const filtered = items.filter((item) => {
      const severityMatch = sev === 'all' || item.severity === sev;
      const hay = `${item.title} ${item.outlet} ${(item.tags || []).join(' ')} ${(item.entities || []).join(' ')}`.toLowerCase();
      return severityMatch && (!term || hay.includes(term));
    });

    if (!filtered.length) {
      list.innerHTML = '<div class="no-data">No matching articles.</div>';
      return;
    }

    list.innerHTML = filtered.map((item) => `
      <div class="card">
        <div style="display:flex;justify-content:space-between;gap:1rem;align-items:center;">
          <a href="${item.url || '#'}" target="_blank" rel="noopener" style="color:#dce9ff;font-weight:700;">${item.title || 'Untitled'}</a>
          <span class="badge badge-${item.severity || 'info'}">${item.severity || 'info'}</span>
        </div>
        <div class="small" style="margin-top:0.3rem;">${item.outlet || 'Unknown outlet'} • ${item.published_at || 'Unknown date'}</div>
        <p>${item.summary || ''}</p>
        <div>${(item.tags || []).map((tag) => `<span class="tag">${tag}</span>`).join('')}</div>
        ${(item.entities || []).length ? `<div class="small" style="margin-top:0.4rem;">Entities: ${(item.entities || []).join(', ')}</div>` : ''}
      </div>
    `).join('');
  }

  severityFilter?.addEventListener('change', draw);
  textFilter?.addEventListener('input', draw);
  draw();
})();
