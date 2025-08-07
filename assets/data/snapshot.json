async function loadSnapshot() {
  const res = await fetch('data/snapshot.json?_=' + Date.now());
  if (!res.ok) throw new Error('Could not load snapshot.json');
  return await res.json();
}

function plotLine(div, x, y, name, ytitle) {
  const data = [{ x, y, type: 'scatter', mode: 'lines', name }];
  const layout = {
    paper_bgcolor: 'rgba(0,0,0,0)',
    plot_bgcolor: 'rgba(0,0,0,0)',
    margin: { t: 10, r: 20, b: 40, l: 55 },
    xaxis: { tickfont: { color: '#cfd7e6' } },
    yaxis: { tickfont: { color: '#cfd7e6' }, title: ytitle || '' },
    legend: { font: { color: '#cfd7e6' } }
  };
  Plotly.newPlot(div, data, layout, { displaylogo: false, responsive: true });
}

function plotBars(div, x, y, name, ytitle) {
  const data = [{ x, y, type: 'bar', name }];
  const layout = {
    paper_bgcolor: 'rgba(0,0,0,0)',
    plot_bgcolor: 'rgba(0,0,0,0)',
    margin: { t: 10, r: 20, b: 40, l: 55 },
    xaxis: { tickfont: { color: '#cfd7e6' } },
    yaxis: { tickfont: { color: '#cfd7e6' }, title: ytitle || '' },
    legend: { font: { color: '#cfd7e6' } }
  };
  Plotly.newPlot(div, data, layout, { displaylogo: false, responsive: true });
}

function fmtEURm(v) {
  return new Intl.NumberFormat('en-IE', { style: 'currency', currency: 'EUR', maximumFractionDigits: 0 }).format(v*1e6);
}

function setKPI(id, value, sub) {
  document.getElementById(id).textContent = value;
  if (sub) document.getElementById(id+'-sub').textContent = sub;
}

(async () => {
  try {
    const snap = await loadSnapshot();
    document.getElementById('lastSnap').textContent = new Date(snap.generated_at).toLocaleString('en-IE');

    // KPIs
    const un = snap.series.unemployment_rate;
    setKPI('kpi-unemp', (un.latest*1).toFixed(1) + '%', un.latest_date);

    if (snap.series.tax_receipts && snap.series.tax_receipts.latest_total_month) {
      const m = snap.series.tax_receipts;
      setKPI('kpi-tax', fmtEURm(m.latest_total_value), m.latest_total_month);
    } else {
      setKPI('kpi-tax', '—', 'No DoF data in snapshot');
    }

    const gni = snap.series.gni_star;
    if (gni) setKPI('kpi-gnistar', fmtEURm(gni.latest_value), gni.latest_year);

    const wages = snap.series.wage_growth;
    if (wages) setKPI('kpi-wages', (wages.latest_yoy*100).toFixed(1) + '%', wages.latest_period);

    // Charts
    plotLine('chart-mdd', snap.series.mdd.x, snap.series.mdd.y, 'MDD (2018=100)', '');
    plotBars('chart-ca', snap.series.current_account.x, snap.series.current_account.y, 'Current account (€bn)', '€bn');
    plotLine('chart-emp-rate', snap.series.employment_rate.x, snap.series.employment_rate.y, 'Employment rate (15–64)', '%');
    plotLine('chart-lr', snap.series.live_register.x, snap.series.live_register.y, 'Live Register (SA, '000)', '000s');

    const h = snap.series.housing;
    Plotly.newPlot('chart-housing', [
      { x: h.completions.x, y: h.completions.y, type: 'bar', name: 'Completions' },
      { x: h.permissions.x, y: h.permissions.y, type: 'scatter', mode: 'lines+markers', name: 'Permissions' }
    ], {
      paper_bgcolor: 'rgba(0,0,0,0)',
      plot_bgcolor: 'rgba(0,0,0,0)',
      margin: { t: 10, r: 20, b: 40, l: 55 },
      xaxis: { tickfont: { color: '#cfd7e6' } },
      yaxis: { tickfont: { color: '#cfd7e6' } },
      legend: { font: { color: '#cfd7e6' } }
    }, { displaylogo: false, responsive: true });

    const hicp = snap.series.hicp;
    Plotly.newPlot('chart-hicp', [
      { x: hicp.ireland.x, y: hicp.ireland.y, type: 'scatter', mode: 'lines', name: 'Ireland HICP y/y' },
      { x: hicp.ea19.x, y: hicp.ea19.y, type: 'scatter', mode: 'lines', name: 'EA19 HICP y/y' }
    ], {
      paper_bgcolor: 'rgba(0,0,0,0)',
      plot_bgcolor: 'rgba(0,0,0,0)',
      margin: { t: 10, r: 20, b: 40, l: 55 },
      xaxis: { tickfont: { color: '#cfd7e6' } },
      yaxis: { tickfont: { color: '#cfd7e6' }, title: '%' },
      legend: { font: { color: '#cfd7e6' } }
    }, { displaylogo: false, responsive: true });

  } catch (e) {
    console.error(e);
    alert('Failed to load snapshot: ' + e.message);
  }
})();
