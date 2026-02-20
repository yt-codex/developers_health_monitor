(async function initListed() {
  const payload = await loadJson('./data/listed.json');
  setLastUpdated('listed-updated', payload);

  const body = document.getElementById('listed-body');
  if (!body) return;

  const rows = payload?.rows || [];
  if (!rows.length) {
    renderNoData('listed-table-wrap', 'Listed developer data unavailable.');
    return;
  }

  function statusClass(status) {
    const s = (status || '').toLowerCase();
    return s === 'green' ? 'status-green' : s === 'amber' ? 'status-amber' : 'status-red';
  }

  body.innerHTML = rows.map((r, idx) => `
    <tr>
      <td>${r.ticker || ''}</td>
      <td>${r.company || ''}</td>
      <td>${r.net_debt_to_equity ?? 'n/a'}</td>
      <td>${r.net_debt_to_ebitda ?? 'n/a'}</td>
      <td>${r.cash_to_short_debt ?? 'n/a'}</td>
      <td>${r.quick_ratio ?? 'n/a'}</td>
      <td><strong>${r.health_score ?? 'n/a'}</strong></td>
      <td class="status-pill ${statusClass(r.status)}">${r.status || 'Unknown'}</td>
      <td><button class="explain-btn" data-target="exp-${idx}">Explain</button></td>
    </tr>
    <tr id="exp-${idx}" style="display:none;">
      <td colspan="9" class="small">${(r.drivers || ['No explanation available.']).join(' ')}</td>
    </tr>
  `).join('');

  body.querySelectorAll('.explain-btn').forEach((btn) => {
    btn.addEventListener('click', () => {
      const target = document.getElementById(btn.dataset.target);
      if (!target) return;
      target.style.display = target.style.display === 'none' ? 'table-row' : 'none';
    });
  });
})();
