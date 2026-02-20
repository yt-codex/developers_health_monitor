(async function initMacro() {
  const payload = await loadJson('./data/macro.json');
  setLastUpdated('macro-updated', payload);

  if (!payload?.series) {
    renderNoData('macro-content', 'Macro data missing or failed to load.');
    return;
  }

  const charts = [
    ['hdb-chart', payload.series.hdb_resale_index],
    ['private-chart', payload.series.private_home_index],
    ['rate-chart', payload.series.mortgage_rate_3m],
  ];

  for (const [canvasId, series] of charts) {
    const canvas = document.getElementById(canvasId);
    if (!canvas || !series?.x?.length || !series?.y?.length) continue;
    new Chart(canvas, {
      type: 'line',
      data: {
        labels: series.x,
        datasets: [{
          label: series.label || 'Series',
          data: series.y,
          tension: 0.25,
          borderColor: '#4ea1ff',
          backgroundColor: 'rgba(78,161,255,0.18)',
          fill: true,
          pointRadius: 2,
        }],
      },
      options: { responsive: true, maintainAspectRatio: false },
    });
  }
})();
