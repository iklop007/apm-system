/* APM 仪表盘。无任何外部依赖,图表是手写的 SVG + hover 提示层。
   系列色槽位固定(蓝→橙→青→黄→品红),按 dataviz 规范做过暗色表面校验,
   任何筛选变化都不会导致同一条线换颜色。 */
'use strict';

const $ = (sel) => document.querySelector(sel);

// 与 style.css 里的 --series-N 保持一致,顺序不能动
const PALETTE = ['#3987e5', '#d95926', '#199e70', '#c98500', '#d55181'];
const KIND_CLASS = { server: 'server', client: 'client', db: 'db', cache: 'cache', internal: 'internal' };

const state = {
  tab: 'overview',
  window: '15m',
  autoRefresh: true,
  services: [],
  traceCursor: null,
  metricNames: [],
  metricSelected: null,
};

/* ---------- 基础工具 ---------- */

async function fetchJSON(url) {
  const resp = await fetch(url);
  if (!resp.ok) throw new Error(`GET ${url} -> ${resp.status}`);
  return resp.json();
}

function esc(s) {
  return String(s ?? '').replace(/[&<>"']/g, (c) => (
    { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]
  ));
}

function fmtTime(us) {
  return new Date(us / 1000).toLocaleTimeString('zh-CN', { hour12: false });
}

function fmtMs(v) {
  if (v == null || isNaN(v)) return '-';
  return v < 10 ? v.toFixed(1) : Math.round(v).toString();
}

function fmtNum(v, digits = 1) {
  if (v == null || isNaN(v)) return '-';
  return v.toFixed(digits);
}

function pct(v) {
  return (v * 100).toFixed(1) + '%';
}

// y 轴上限取整到 1/2/2.5/5 * 10^k,别让刻度乱糟糟的
function niceMax(v) {
  if (v <= 0) return 1;
  const pow = Math.pow(10, Math.floor(Math.log10(v)));
  for (const m of [1, 2, 2.5, 5, 10]) {
    if (m * pow >= v) return m * pow;
  }
  return 10 * pow;
}

function badge(status) {
  const ok = status === 'ok';
  return `<span class="badge ${ok ? 'ok' : 'err'}">${ok ? '正常' : '异常'}</span>`;
}

function levelBadge(level) {
  const cls = { error: 'err', warn: 'warn', info: 'ok', debug: 'ok' }[level] || 'ok';
  return `<span class="badge ${cls}">${esc(level)}</span>`;
}

/* ---------- 折线图 ---------- */

// series: [{name, color, values:[]}],buckets: [µs]。单测数据量小,直接拼 SVG 字符串。
function lineChart(container, opts) {
  const { buckets, series, yFmt, yLabel } = opts;
  container.innerHTML = '';

  const n = buckets.length;
  if (!n) {
    container.innerHTML = '<div class="empty">暂无数据,等 agent 上报后会自动出现</div>';
    return;
  }

  const W = Math.max(container.clientWidth, 400);
  const H = 230;
  const ml = 56, mr = 18, mt = 30, mb = 28;
  const iw = W - ml - mr, ih = H - mt - mb;

  let maxV = 0;
  for (const s of series) for (const v of s.values) if (v > maxV) maxV = v;
  const yMax = niceMax(maxV);

  const x = (i) => ml + (n === 1 ? iw / 2 : (i * iw) / (n - 1));
  const y = (v) => mt + ih - (v / yMax) * ih;

  let svg = `<svg viewBox="0 0 ${W} ${H}" role="img" aria-label="${esc(yLabel)}折线图">`;

  // 网格 + y 刻度(4 等分)
  const ticks = 4;
  for (let i = 0; i <= ticks; i++) {
    const gv = (yMax / ticks) * i;
    const gy = mt + ih - (i * ih) / ticks;
    svg += `<line x1="${ml}" y1="${gy}" x2="${W - mr}" y2="${gy}" stroke="var(--grid)" stroke-width="1"/>`;
    svg += `<text x="${ml - 8}" y="${gy + 4}" text-anchor="end" font-size="11" fill="var(--muted)" style="font-variant-numeric:tabular-nums">${fmtNum(gv)}</text>`;
  }

  // x 轴时间标签,最多 6 个
  const step = Math.max(1, Math.ceil(n / 6));
  for (let i = 0; i < n; i += step) {
    const label = fmtTime(buckets[i]).slice(0, 8);
    svg += `<text x="${x(i)}" y="${H - 8}" text-anchor="middle" font-size="11" fill="var(--muted)" style="font-variant-numeric:tabular-nums">${esc(label)}</text>`;
  }

  // 图例(多系列必带;单系列靠标题说明,不画)
  if (series.length > 1) {
    let lx = ml;
    for (const s of series) {
      svg += `<rect x="${lx}" y="8" width="10" height="10" rx="2" fill="${s.color}"/>`;
      svg += `<text x="${lx + 15}" y="17" font-size="12" fill="var(--ink-2)">${esc(s.name)}</text>`;
      lx += 15 + s.name.length * 13 + 22;
    }
  }

  // 折线 + 数据点(细标记,2px 线)
  for (const s of series) {
    const pts = s.values.map((v, i) => `${x(i)},${y(v)}`).join(' ');
    svg += `<polyline points="${pts}" fill="none" stroke="${s.color}" stroke-width="2" stroke-linejoin="round"/>`;
    for (let i = 0; i < n; i++) {
      svg += `<circle cx="${x(i)}" cy="${y(s.values[i])}" r="3" fill="${s.color}" stroke="var(--surface)" stroke-width="1"/>`;
    }
    // 末端直接标注:最后一个点加大,旁边写数值,文字用墨色不带系列色
    const li = n - 1;
    svg += `<circle cx="${x(li)}" cy="${y(s.values[li])}" r="4.5" fill="${s.color}" stroke="var(--surface)" stroke-width="1.5"/>`;
    svg += `<text x="${x(li) + 9}" y="${y(s.values[li]) + 4}" font-size="11" fill="var(--ink-2)" style="font-variant-numeric:tabular-nums">${yFmt ? yFmt(s.values[li]) : fmtNum(s.values[li])}</text>`;
  }

  svg += '</svg>';
  container.innerHTML = svg;

  // hover:十字线 + 跟随提示框
  const tip = document.createElement('div');
  tip.className = 'chart-tip';
  container.appendChild(tip);

  const svgEl = container.querySelector('svg');
  const rect = svgEl.getBoundingClientRect();
  const scaleX = W / rect.width;

  svgEl.addEventListener('mousemove', (e) => {
    const px = (e.clientX - rect.left) * scaleX;
    if (px < ml || px > W - mr) { tip.style.display = 'none'; return; }
    let idx = Math.round(((px - ml) / iw) * (n - 1));
    idx = Math.max(0, Math.min(n - 1, idx));

    const rows = series.map((s) => `
      <div class="tip-row">
        <span class="tip-swatch" style="background:${s.color}"></span>
        <span class="tip-name">${esc(s.name)}</span>
        <span class="tip-val">${yFmt ? yFmt(s.values[idx]) : fmtNum(s.values[idx])}</span>
      </div>`).join('');

    tip.innerHTML = `<div class="tip-time">${fmtTime(buckets[idx])}</div>${rows}`;
    tip.style.display = 'block';
    const tx = (x(idx) / W) * rect.width;
    tip.style.left = (tx + 14 > rect.width - 150 ? tx - 164 : tx + 14) + 'px';
    tip.style.top = '6px';
  });
  svgEl.addEventListener('mouseleave', () => { tip.style.display = 'none'; });
}

/* ---------- 各页面加载 ---------- */

async function loadOverview() {
  const ov = await fetchJSON(`/api/v1/overview?window=${state.window}`);
  const t = ov.totals;

  $('#statCards').innerHTML = `
    <div class="stat"><div class="stat-label">QPS</div><div class="stat-value">${fmtNum(t.qps)}</div><div class="stat-sub">${ov.window_seconds}s 窗口</div></div>
    <div class="stat"><div class="stat-label">平均耗时</div><div class="stat-value">${fmtMs(t.avg_ms)}<span class="stat-sub"> ms</span></div></div>
    <div class="stat"><div class="stat-label">P95</div><div class="stat-value">${fmtMs(t.p95_ms)}<span class="stat-sub"> ms</span></div></div>
    <div class="stat"><div class="stat-label">P99</div><div class="stat-value">${fmtMs(t.p99_ms)}<span class="stat-sub"> ms</span></div></div>
    <div class="stat"><div class="stat-label">错误率</div><div class="stat-value">${pct(t.error_rate)}</div><div class="stat-sub">${t.errors} / ${t.requests} 请求</div></div>
    <div class="stat"><div class="stat-label">请求总量</div><div class="stat-value">${t.requests}</div><div class="stat-sub">${ov.top_services.length} 个活跃服务</div></div>`;

  // 延迟:avg + p95,单位一致可以共用一个轴
  lineChart($('#chartLatency'), {
    buckets: ov.series.buckets,
    series: [
      { name: '平均', color: PALETTE[0], values: ov.series.avg_ms },
      { name: 'P95', color: PALETTE[1], values: ov.series.p95_ms },
    ],
    yLabel: '延迟(ms)',
    yFmt: (v) => fmtMs(v) + ' ms',
  });

  // 请求数按服务拆,最多画 3 条(全对校验上限),剩下的折叠进表格
  const svcSeries = Object.entries(ov.service_series || {}).map(([name, values], i) => ({
    name, values, color: PALETTE[i % PALETTE.length],
  }));
  lineChart($('#chartRequests'), {
    buckets: ov.series.buckets,
    series: svcSeries.length ? svcSeries : [{ name: '请求数', color: PALETTE[0], values: ov.series.requests }],
    yLabel: '请求数',
    yFmt: (v) => fmtNum(v, 0),
  });

  lineChart($('#chartErrors'), {
    buckets: ov.series.buckets,
    series: [{ name: '错误率', color: PALETTE[1], values: ov.series.error_rate }],
    yLabel: '错误率',
    yFmt: pct,
  });

  // Top 服务:横向条形,单一色相表达数量,数值直接标注
  $('#topServices').innerHTML = ov.top_services.map((s) => {
    const max = Math.max(1, ...ov.top_services.map((x) => x.requests));
    return `
      <div class="svc-row">
        <div class="svc-head">
          <span class="svc-name">${esc(s.service)}</span>
          <span class="svc-num">${s.requests} 请求 · 平均 ${fmtMs(s.avg_ms)} ms · P95 ${fmtMs(s.p95_ms)} ms · 错误 ${pct(s.errors / Math.max(1, s.requests))}</span>
        </div>
        <div class="svc-bar"><div style="width:${(s.requests / max) * 100}%"></div></div>
      </div>`;
  }).join('') || '<div class="empty">暂无数据</div>';

  $('#topSlow').innerHTML = renderTraceRows(ov.top_slow);
}

function renderTraceRows(items) {
  if (!items.length) return '<div class="empty">暂无数据</div>';
  return `<table class="table"><thead><tr><th>时间</th><th>服务</th><th>事务</th><th>耗时</th><th>状态</th></tr></thead><tbody>` +
    items.map((tr) => `
      <tr class="clickable" data-trace="${esc(tr.trace_id)}">
        <td>${fmtTime(tr.start)}</td>
        <td>${esc(tr.service)}</td>
        <td>${esc(tr.name)}</td>
        <td class="num">${fmtMs(tr.duration_ms)} ms</td>
        <td>${badge(tr.status)}</td>
      </tr>`).join('') + '</tbody></table>';
}

async function loadTraces(reset) {
  const q = new URLSearchParams();
  const svc = $('#traceService').value;
  const status = $('#traceStatus').value;
  const minDur = $('#traceMinDur').value;
  const search = $('#traceSearch').value;
  if (svc) q.set('service', svc);
  if (status) q.set('status', status);
  if (minDur && +minDur > 0) q.set('min_duration_ms', minDur);
  if (search) q.set('q', search);
  if (!reset && state.traceCursor) q.set('before', state.traceCursor);
  q.set('limit', 50);

  const data = await fetchJSON('/api/v1/traces?' + q.toString());
  const items = data.items || [];

  if (reset) $('#traceTable tbody').innerHTML = '';
  if (!items.length && reset) {
    $('#traceTable tbody').innerHTML = '<tr><td colspan="8" class="empty">暂无匹配的链路</td></tr>';
  } else {
    $('#traceTable tbody').insertAdjacentHTML('beforeend', items.map((tr) => `
      <tr class="clickable" data-trace="${esc(tr.trace_id)}">
        <td>${fmtTime(tr.start)}</td>
        <td><span class="trace-link">${esc(tr.trace_id.slice(0, 8))}</span></td>
        <td>${esc(tr.service)}</td>
        <td>${esc(tr.name)}</td>
        <td class="num">${fmtMs(tr.duration_ms)} ms</td>
        <td>${badge(tr.status)}</td>
        <td>${tr.http_status}</td>
        <td class="num">${tr.span_count}</td>
      </tr>`).join(''));
  }

  state.traceCursor = items.length ? items[items.length - 1].start : null;
  $('#traceMore').style.display = items.length >= 50 ? '' : 'none';
}

async function loadMetricNames() {
  const data = await fetchJSON('/api/v1/metrics/names');
  state.metricNames = data.metrics || [];

  // 按类型分组展示,每次只选一个指标,避免把不同量纲塞进一张图
  const groups = {};
  for (const m of state.metricNames) {
    (groups[m.type] = groups[m.type] || []).push(m);
  }
  const typeLabel = { counter: '计数器', gauge: '仪表值', histogram: '直方图' };

  $('#metricList').innerHTML = Object.entries(groups).map(([type, list]) => `
    <div class="muted" style="padding:8px 8px 2px;font-size:12px">${typeLabel[type] || type}</div>
    ${list.map((m) => `
      <div class="metric-item ${state.metricSelected === m.name ? 'selected' : ''}" data-metric="${esc(m.name)}" data-type="${esc(m.type)}">
        <span class="m-name">${esc(m.name)}</span>
        <span class="m-type">${esc(type)}</span>
      </div>`).join('')}`).join('') || '<div class="empty">暂无指标</div>';

  // 之前选中的指标没了就清掉
  if (state.metricSelected && !state.metricNames.some((m) => m.name === state.metricSelected)) {
    state.metricSelected = null;
    $('#chartMetric').innerHTML = '<div class="empty">选择一个指标</div>';
  }
  if (!state.metricSelected && state.metricNames.length) {
    state.metricSelected = state.metricNames[0].name;
    document.querySelectorAll('.metric-item').forEach((el) => {
      if (el.dataset.metric === state.metricSelected) el.classList.add('selected');
    });
  }
}

async function loadMetrics() {
  if (!state.metricSelected) return;
  const meta = state.metricNames.find((m) => m.name === state.metricSelected);
  if (!meta) return;

  const q = new URLSearchParams({ names: meta.name });
  if (meta.type === 'counter' && $('#metricRate').checked) q.set('rate', 'true');
  if (meta.type === 'histogram') q.set('quantile', $('#metricQuantile').value);

  const data = await fetchJSON('/api/v1/metrics?' + q.toString());
  $('#metricChartTitle').innerHTML = `${esc(meta.name)} <span class="muted">${
    meta.type === 'counter' && q.get('rate') ? '每秒速率' :
    meta.type === 'histogram' ? '分位数 ms' : '原始值'}</span>`;

  const series = (data.series || []).map((s) => ({
    name: Object.entries(s.labels || {}).map(([k, v]) => `${k}=${v}`).join(',') || s.name,
    color: PALETTE[0],
    values: s.points.map((p) => p[1]),
  }));
  lineChart($('#chartMetric'), {
    buckets: (data.series || []).flatMap((s) => s.points.map((p) => p[0])),
    series,
    yLabel: meta.name,
  });
}

async function loadLogs() {
  const q = new URLSearchParams({ limit: 200 });
  const level = $('#logLevel').value;
  const svc = $('#logService').value;
  const search = $('#logSearch').value;
  if (level) q.set('level', level);
  if (svc) q.set('service', svc);
  if (search) q.set('q', search);

  const data = await fetchJSON('/api/v1/logs?' + q.toString());
  const logs = data.logs || [];
  $('#logTable tbody').innerHTML = logs.map((l) => `
    <tr>
      <td>${fmtTime(l.time)}</td>
      <td>${levelBadge(l.level)}</td>
      <td>${esc(l.service)}</td>
      <td>${esc(l.message)}</td>
      <td>${l.trace_id ? `<span class="trace-link" data-trace="${esc(l.trace_id)}">${esc(l.trace_id.slice(0, 8))}</span>` : '-'}</td>
    </tr>`).join('') || '<tr><td colspan="5" class="empty">暂无日志</td></tr>';
}

async function loadServices() {
  const data = await fetchJSON('/api/v1/services');
  const svcs = data.services || [];
  $('#serviceTable tbody').innerHTML = svcs.map((s) => `
    <tr>
      <td><b>${esc(s.name)}</b></td>
      <td>${esc(s.host || '-')}</td>
      <td>${esc(s.version || '-')}</td>
      <td class="num">${s.requests}</td>
      <td class="num">${s.errors}</td>
      <td class="num">${fmtMs(s.TotalDurMS)} ms</td>
      <td>${fmtTime(s.last_seen)}</td>
    </tr>`).join('') || '<tr><td colspan="7" class="empty">暂无服务上报</td></tr>';
}

/* ---------- 链路详情 + 瀑布图 ---------- */

async function openTrace(traceID) {
  let rec;
  try {
    rec = await fetchJSON('/api/v1/traces/' + encodeURIComponent(traceID));
  } catch {
    alert('链路不存在或已过期');
    return;
  }

  const txs = rec.Transactions || [];
  const start = Math.min(...txs.map((t) => t.StartTime));
  const end = Math.max(...txs.map((t) => t.StartTime + t.Duration));
  const totalMS = (end - start) / 1000;
  const errTx = txs.some((t) => t.Status === 'error');

  $('#drawerTitle').textContent = `${txs[0]?.Name || '链路'} · ${traceID.slice(0, 8)}`;

  let body = `
    <div class="wf-meta">
      <span>Trace ID: <b>${esc(traceID)}</b></span>
      <span>耗时: <b>${fmtMs(totalMS)} ms</b></span>
      <span>事务数: <b>${txs.length}</b></span>
      <span>状态: ${badge(errTx ? 'error' : 'ok')}</span>
      <span>时间: <b>${fmtTime(start)}</b></span>
    </div>`;

  for (const tx of txs) {
    body += `
      <div class="wf-tx">
        <div class="wf-tx-head">
          <span>${esc(tx.Service)} · ${esc(tx.Name)}</span>
          <span class="tx-dur">${fmtMs(tx.Duration / 1000)} ms · HTTP ${tx.HTTPStatus} · ${badge(tx.Status)}</span>
        </div>
        ${tx.Tags ? `<div class="tag-chips">${Object.entries(tx.Tags).map(([k, v]) => `<span class="chip"><b>${esc(k)}</b>=${esc(v)}</span>`).join('')}</div>` : ''}`;

    const spans = [...(tx.Spans || [])].sort((a, b) => a.StartOffset - b.StartOffset);
    for (const sp of spans) {
      const cls = sp.Status === 'error' ? 'err' : (KIND_CLASS[sp.Kind] || 'server');
      const left = ((sp.StartOffset / 1000) / totalMS) * 100;
      const width = Math.max(0.3, (sp.Duration / 1000 / totalMS) * 100);
      body += `
        <div class="wf-row" title="${esc(sp.Name)}: ${fmtMs(sp.Duration / 1000)} ms">
          <div class="wf-label">
            <span>${esc(sp.Name)}</span>
            <span class="wf-kind">${esc(sp.Kind || 'server')}</span>
          </div>
          <div class="wf-lane">
            <div class="wf-bar ${cls}" style="left:${left}%;width:${width}%"></div>
          </div>
        </div>`;
    }
    body += '</div>';
  }

  $('#drawerBody').innerHTML = body || '<div class="empty">暂无数据</div>';
  $('#drawer').classList.remove('hidden');
  $('#drawerBackdrop').classList.remove('hidden');
}

function closeDrawer() {
  $('#drawer').classList.add('hidden');
  $('#drawerBackdrop').classList.add('hidden');
}

/* ---------- 服务下拉 ---------- */

async function refreshServiceOptions() {
  const data = await fetchJSON('/api/v1/services');
  state.services = data.services || [];
  const opts = state.services.map((s) => `<option value="${esc(s.name)}">${esc(s.name)}</option>`).join('');
  $('#traceService').innerHTML = '<option value="">全部</option>' + opts;
  $('#logService').innerHTML = '<option value="">全部</option>' + opts;
}

/* ---------- 轮询与交互 ---------- */

const loaders = {
  overview: async () => { await loadOverview(); },
  traces: async () => { await loadTraces(true); },
  metrics: async () => { await loadMetricNames(); await loadMetrics(); },
  logs: async () => { await loadLogs(); },
  services: async () => { await loadServices(); },
};

async function refresh(forceServices = false) {
  try {
    if (forceServices || state.tab === 'traces' || state.tab === 'logs') {
      await refreshServiceOptions();
    }
    await loaders[state.tab]();
    $('#lastRefresh').textContent = '更新于 ' + fmtTime(Date.now() * 1000);
  } catch (err) {
    $('#lastRefresh').textContent = '刷新失败';
    console.error(err);
  }
}

function switchTab(tab) {
  state.tab = tab;
  document.querySelectorAll('.tab').forEach((el) => el.classList.toggle('active', el.dataset.tab === tab));
  document.querySelectorAll('.page').forEach((el) => el.classList.toggle('active', el.id === 'page-' + tab));
  refresh();
}

document.querySelectorAll('.tab').forEach((el) => {
  el.addEventListener('click', () => switchTab(el.dataset.tab));
});

$('#window').addEventListener('change', (e) => {
  state.window = e.target.value;
  refresh();
});

$('#autoRefresh').addEventListener('change', (e) => {
  state.autoRefresh = e.target.checked;
});

// 链路表格和日志里的 trace 链接都走事件委托
document.addEventListener('click', (e) => {
  const row = e.target.closest('[data-trace]');
  if (row) openTrace(row.dataset.trace);
});

$('#traceMore').addEventListener('click', () => loadTraces(false));

['#traceService', '#traceStatus', '#traceMinDur', '#traceSearch'].forEach((sel) => {
  $(sel).addEventListener('change', () => loadTraces(true));
});
$('#traceSearch').addEventListener('keydown', (e) => { if (e.key === 'Enter') loadTraces(true); });

['#logLevel', '#logService', '#logSearch'].forEach((sel) => {
  $(sel).addEventListener('change', loadLogs);
});
$('#logSearch').addEventListener('keydown', (e) => { if (e.key === 'Enter') loadLogs(); });

$('#metricList').addEventListener('click', (e) => {
  const item = e.target.closest('.metric-item');
  if (!item) return;
  document.querySelectorAll('.metric-item').forEach((el) => el.classList.remove('selected'));
  item.classList.add('selected');
  state.metricSelected = item.dataset.metric;
  loadMetrics();
});
$('#metricRate').addEventListener('change', loadMetrics);
$('#metricQuantile').addEventListener('change', loadMetrics);

$('#drawerClose').addEventListener('click', closeDrawer);
$('#drawerBackdrop').addEventListener('click', closeDrawer);
document.addEventListener('keydown', (e) => { if (e.key === 'Escape') closeDrawer(); });

// 启动
refresh(true);
setInterval(() => { if (state.autoRefresh) refresh(); }, 5000);
