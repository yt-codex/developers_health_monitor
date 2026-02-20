(async function initMacro() {
  const [payload, status] = await Promise.all([
    loadJson('./data/macro.json'),
    loadJson('./data/status.json'),
  ]);

  const generated = payload?.meta?.generated_utc || payload?.updated_at;
  const updatedEl = document.getElementById('macro-updated');
  if (updatedEl) {
    updatedEl.textContent = generated
      ? `Last updated: ${new Date(generated).toLocaleString()}`
      : 'Last updated: unavailable';
  }

  const warningEl = document.getElementById('macro-warning');
  const failed = Object.entries(status?.series_status || {})
    .filter(([, v]) => !v?.ok)
    .map(([k]) => k);
  if (warningEl) {
    if (failed.length) {
      warningEl.textContent = `Warning: ${failed.length} series unavailable (${failed.slice(0, 5).join(', ')}${failed.length > 5 ? ', ...' : ''}).`;
      warningEl.style.display = 'block';
    } else {
      warningEl.style.display = 'none';
    }
  }

  const series = payload?.series || {};

  const fallback = (label) => ({
    display_name: `${label} (Mock fallback)`,
    data: [
      { period: '2025-Q1', value: 100 },
      { period: '2025-Q2', value: 101.2 },
      { period: '2025-Q3', value: 102.1 },
      { period: '2025-Q4', value: 102.8 },
    ],
    last_observation_period: '2025-Q4',
  });

  const chartDefs = [
    {
      id: 'sora-chart',
      titleId: 'sora-last',
      datasets: [series.sora_level || fallback('SORA')],
    },
    {
      id: 'yields-chart',
      titleId: 'yields-last',
      datasets: [
        series.sgs_yield_10y || fallback('10Y Yield'),
        series.sgs_yield_2y || series.sgs_yield_1y || fallback('2Y/1Y Yield'),
        series.yield_curve_slope || fallback('Yield Curve Slope'),
      ],
    },
    {
      id: 'price-chart',
      titleId: 'price-last',
      datasets: [series.private_resi_price_index || fallback('Private Residential Price Index')],
    },
    {
      id: 'rental-chart',
      titleId: 'rental-last',
      datasets: [series.private_resi_rental_index || fallback('Private Residential Rental Index')],
    },
    {
      id: 'transactions-chart',
      titleId: 'transactions-last',
      datasets: [
        series.private_resi_transactions || fallback('Private Residential Transactions'),
        series.private_resi_transactions_ma3 || null,
      ].filter(Boolean),
    },
    {
      id: 'dev-sales-chart',
      titleId: 'dev-sales-last',
      datasets: [
        series.dev_sales_uncompleted_sold || fallback('Uncompleted Sold'),
        series.dev_sales_completed_sold || null,
      ].filter(Boolean),
    },
    {
      id: 'construction-chart',
      titleId: 'construction-last',
      datasets: [series.construction_material_prices || fallback('Construction Material Prices')],
    },
  ];

  const colors = ['#4ea1ff', '#8bc34a', '#f59e0b', '#ef4444'];

  for (const def of chartDefs) {
    const canvas = document.getElementById(def.id);
    if (!canvas) continue;

    const labels = Array.from(
      new Set(def.datasets.flatMap((s) => (s?.data || []).map((p) => p.period)))
    ).sort();

    if (!labels.length) continue;

    const datasets = def.datasets.map((s, idx) => {
      const byPeriod = new Map((s.data || []).map((p) => [p.period, p.value]));
      return {
        label: s.display_name || `Series ${idx + 1}`,
        data: labels.map((l) => (byPeriod.has(l) ? byPeriod.get(l) : null)),
        borderColor: colors[idx % colors.length],
        backgroundColor: 'rgba(78,161,255,0.1)',
        tension: 0.25,
        spanGaps: true,
        pointRadius: 2,
      };
    });

    const lastEl = document.getElementById(def.titleId);
    if (lastEl) {
      const latest = def.datasets
        .map((d) => d.last_observation_period)
        .filter(Boolean)
        .join(' | ');
      lastEl.textContent = latest ? `Latest: ${latest}` : 'Latest: unavailable';
    }

    new Chart(canvas, {
      type: 'line',
      data: { labels, datasets },
      options: { responsive: true, maintainAspectRatio: false },
    });
  }
})();
