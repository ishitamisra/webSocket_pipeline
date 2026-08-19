(() => {
  "use strict";

  const state = {
    symbols: [],
    timeframes: [],
    symbol: null,
    timeframe: 60,
    lastPrice: null,
    ws: null,
    wsRetryDelay: 1000,
    chartFollowing: false,
  };

  const el = (id) => document.getElementById(id);
  const fmt = (n, d = 2) => (n === null || n === undefined || Number.isNaN(n) ? "--" : Number(n).toFixed(d));
  const fmtInt = (n) => (n === null || n === undefined ? "--" : Number(n).toLocaleString());

  // ---------- Chart setup ----------

  const chartEl = el("chart");
  const chart = LightweightCharts.createChart(chartEl, {
    layout: {
      background: { type: "solid", color: "#11161f" },
      textColor: "#8a96a8",
      fontFamily: "SF Mono, Roboto Mono, ui-monospace, monospace",
      fontSize: 11,
    },
    grid: {
      vertLines: { color: "#1a212e" },
      horzLines: { color: "#1a212e" },
    },
    crosshair: { mode: LightweightCharts.CrosshairMode.Normal },
    rightPriceScale: { borderColor: "#232b3a" },
    timeScale: { borderColor: "#232b3a", timeVisible: true, secondsVisible: true },
  });

  const candleSeries = chart.addCandlestickSeries({
    upColor: "#22c55e",
    downColor: "#ef4444",
    borderVisible: false,
    wickUpColor: "#22c55e",
    wickDownColor: "#ef4444",
  });

  const vwapSeries = chart.addLineSeries({ color: "#a78bfa", lineWidth: 1, priceLineVisible: false, lastValueVisible: false });
  const sma20Series = chart.addLineSeries({ color: "#f59e0b", lineWidth: 1, priceLineVisible: false, lastValueVisible: false });
  const sma50Series = chart.addLineSeries({ color: "#38bdf8", lineWidth: 1, priceLineVisible: false, lastValueVisible: false });

  new ResizeObserver(() => {
    chart.applyOptions({ width: chartEl.clientWidth, height: chartEl.clientHeight });
  }).observe(chartEl);

  // ---------- Data loading ----------

  async function loadSymbols() {
    const res = await fetch("/api/symbols");
    const data = await res.json();
    state.symbols = data.symbols;
    state.timeframes = data.timeframes_sec;
    state.symbol = state.symbols[0];
    renderSymbolTabs();
  }

  function renderSymbolTabs() {
    const container = el("symbol-tabs");
    container.innerHTML = "";
    for (const sym of state.symbols) {
      const btn = document.createElement("button");
      btn.textContent = sym.replace("-", "/");
      btn.dataset.symbol = sym;
      if (sym === state.symbol) btn.classList.add("active");
      btn.addEventListener("click", () => selectSymbol(sym));
      container.appendChild(btn);
    }
  }

  function selectSymbol(sym) {
    state.symbol = sym;
    state.lastPrice = null;
    document.querySelectorAll("#symbol-tabs button").forEach((b) => {
      b.classList.toggle("active", b.dataset.symbol === sym);
    });
    el("tape").innerHTML = "";
    loadCandles();
  }

  function selectTimeframe(tf) {
    state.timeframe = tf;
    document.querySelectorAll("#timeframe-tabs button").forEach((b) => {
      b.classList.toggle("active", Number(b.dataset.tf) === tf);
    });
    loadCandles();
  }

  async function loadCandles() {
    const res = await fetch(`/api/candles/${state.symbol}?timeframe=${state.timeframe}&limit=300`);
    const data = await res.json();
    const bars = data.candles.map((c) => ({
      time: Math.floor(c.bucket_start_ms / 1000),
      open: c.open,
      high: c.high,
      low: c.low,
      close: c.close,
    }));
    candleSeries.setData(bars);
    vwapSeries.setData([]);
    sma20Series.setData([]);
    sma50Series.setData([]);
    if (bars.length) {
      state.lastPrice = bars[bars.length - 1].close;
      el("last-price").textContent = fmt(state.lastPrice, 2);
    }
    // A fresh series (especially one seeded with zero bars, e.g. right
    // after the server starts) doesn't automatically enter "follow live
    // data" mode -- fit the view explicitly, and let the next live
    // candle_update do it again in case there was no history yet.
    chart.timeScale().fitContent();
    state.chartFollowing = false;
  }

  // ---------- WebSocket live stream ----------

  function connectWs() {
    const proto = location.protocol === "https:" ? "wss" : "ws";
    const ws = new WebSocket(`${proto}://${location.host}/ws/stream`);
    state.ws = ws;

    ws.onopen = () => {
      setConnStatus(true);
      state.wsRetryDelay = 1000;
    };
    ws.onclose = () => {
      setConnStatus(false);
      setTimeout(connectWs, state.wsRetryDelay);
      state.wsRetryDelay = Math.min(state.wsRetryDelay * 2, 15000);
    };
    ws.onerror = () => ws.close();
    ws.onmessage = (ev) => {
      let msg;
      try {
        msg = JSON.parse(ev.data);
      } catch {
        return;
      }
      handleEvent(msg);
    };
  }

  function setConnStatus(live) {
    const container = el("conn-status");
    container.classList.toggle("live", live);
    container.classList.toggle("down", !live);
    container.querySelector(".conn-label").textContent = live ? "live" : "reconnecting…";
  }

  function handleEvent(msg) {
    switch (msg.type) {
      case "tick":
        if (msg.data.symbol === state.symbol) handleTick(msg.data);
        break;
      case "candle_update":
      case "candle_closed":
        if (msg.data.symbol === state.symbol && msg.data.timeframe_sec === state.timeframe) {
          candleSeries.update({
            time: Math.floor(msg.data.bucket_start_ms / 1000),
            open: msg.data.open,
            high: msg.data.high,
            low: msg.data.low,
            close: msg.data.close,
          });
          if (!state.chartFollowing) {
            // First live bar since the last setData() call -- make sure
            // it's actually in view, then let the library's own
            // follow-latest-bar behavior take over from here.
            chart.timeScale().fitContent();
            state.chartFollowing = true;
          }
        }
        break;
      case "stats":
        updateHealthFromStats(msg.data);
        break;
    }
  }

  function handleTick(tick) {
    const prevPrice = state.lastPrice;
    state.lastPrice = tick.price;
    el("last-price").textContent = fmt(tick.price, 2);

    const changeEl = el("price-change");
    if (prevPrice !== null) {
      const diff = tick.price - prevPrice;
      if (diff !== 0) {
        changeEl.textContent = (diff > 0 ? "+" : "") + fmt(diff, 2);
        changeEl.className = "price-change " + (diff > 0 ? "up" : "down");
      }
    }

    const t = Math.floor(tick.event_time_ms / 1000);
    vwapSeries.update({ time: t, value: tick.vwap });
    if (tick.moving_averages.sma_20 != null) sma20Series.update({ time: t, value: tick.moving_averages.sma_20 });
    if (tick.moving_averages.sma_50 != null) sma50Series.update({ time: t, value: tick.moving_averages.sma_50 });

    el("m-vwap").textContent = fmt(tick.vwap, 2);
    el("m-sma20").textContent = fmt(tick.moving_averages.sma_20, 2);
    el("m-sma50").textContent = fmt(tick.moving_averages.sma_50, 2);
    el("m-ema20").textContent = fmt(tick.moving_averages.ema_20, 2);
    el("m-ema50").textContent = fmt(tick.moving_averages.ema_50, 2);

    addTapeRow(tick);
  }

  function addTapeRow(tick) {
    const tape = el("tape");
    const row = document.createElement("div");
    row.className = "tape-row";
    const time = new Date(tick.event_time_ms).toLocaleTimeString([], { hour12: false });
    row.innerHTML = `
      <span class="${tick.side === "buy" ? "side-buy" : "side-sell"}">${fmt(tick.price, 2)}</span>
      <span class="qty">${fmt(tick.quantity, 4)}</span>
      <span class="time">${time}</span>
    `;
    tape.appendChild(row);
    while (tape.children.length > 120) tape.removeChild(tape.firstChild);
  }

  function updateHealthFromStats(statsList) {
    const s = statsList.find((x) => x.symbol === state.symbol);
    if (!s) return;
    el("m-rate").textContent = fmt(s.msgs_per_sec, 1);
    el("h-buffer-text").textContent = `${fmtInt(s.buffer_depth)} / ${fmtInt(s.buffer_capacity)}`;
    const pct = s.buffer_capacity ? (100 * s.buffer_depth) / s.buffer_capacity : 0;
    const fill = el("h-buffer-fill");
    fill.style.width = `${Math.min(pct, 100)}%`;
    fill.style.background = pct > 80 ? "var(--red)" : pct > 40 ? "var(--amber)" : "var(--green)";
    el("h-dropped").textContent = fmtInt(s.dropped_ring_buffer);
    el("h-ooo").textContent = fmtInt(s.out_of_order_count);
    el("h-gaps").textContent = fmtInt(s.gap_count);
  }

  async function pollHealth() {
    try {
      const res = await fetch("/api/stats");
      const data = await res.json();
      const exEl = el("h-exchange");
      exEl.textContent = data.exchange_connected ? "connected" : "disconnected";
      exEl.className = "health-value " + (data.exchange_connected ? "ok" : "bad");
      el("h-reconnects").textContent = fmtInt(data.exchange_reconnects);
      el("h-clients").textContent = fmtInt(data.connected_clients);
      const uptime = data.uptime_sec;
      const m = Math.floor(uptime / 60);
      const s = Math.floor(uptime % 60);
      el("h-uptime").textContent = `${m}m ${s}s`;
      updateHealthFromStats(data.symbols);
    } catch {
      // ignore transient fetch errors; ws status dot already reflects link state
    }
  }

  // ---------- Init ----------

  document.querySelectorAll("#timeframe-tabs .tf-btn").forEach((btn) => {
    btn.addEventListener("click", () => selectTimeframe(Number(btn.dataset.tf)));
  });

  (async () => {
    await loadSymbols();
    await loadCandles();
    connectWs();
    pollHealth();
    setInterval(pollHealth, 5000);
  })();
})();
