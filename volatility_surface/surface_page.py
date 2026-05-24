"""Presentation layer for the implied-volatility surface endpoint.

Turns the raw QuantLib surface grid into a fully styled, self-contained HTML
page: a dark "instrument readout" wrapping an interactive Plotly surface that
has been restyled to match the page (custom plasma colorscale, transparent
canvas, monospace axes). See ``render`` in ``__init__`` for the entry point.
"""

import html
from string import Template
from typing import Optional

import numpy as np
import plotly.graph_objs as go
import plotly.io as pio

try:  # pin the CDN bundle to the plotly.py we ship, so the JSON schema matches
    from plotly.offline.offline import get_plotlyjs_version
    PLOTLY_VERSION = get_plotlyjs_version()
except Exception:  # pragma: no cover - defensive
    PLOTLY_VERSION = "3.0.1"
PLOTLY_CDN = f"https://cdn.plot.ly/plotly-{PLOTLY_VERSION}.min.js"


# Calm -> stressed. Cool indigo at low vol, warm amber/cream at high vol.
SURFACE_COLORSCALE = [
    [0.00, "#1b1f47"],
    [0.25, "#4b3a8f"],
    [0.50, "#8e3f9e"],
    [0.72, "#e0653f"],
    [0.88, "#f3b13e"],
    [1.00, "#ffeec2"],
]

_AXIS = dict(
    backgroundcolor="rgba(0,0,0,0)",
    gridcolor="rgba(236,232,223,0.07)",
    zerolinecolor="rgba(236,232,223,0.12)",
    showbackground=True,
    color="#7d8290",
    title_font=dict(family="IBM Plex Mono, monospace", size=11, color="#9aa0ad"),
    tickfont=dict(family="IBM Plex Mono, monospace", size=10, color="#6b7080"),
)

# Curated popular, liquid optionable symbols for the page's ticker dropdown.
# The engine can fetch any CBOE-listed symbol on demand; this is a grouped
# convenience list (50 names) for the <select>.
POPULAR_TICKERS = [
    ("Index ETFs", ["SPY", "QQQ", "IWM", "DIA"]),
    ("Technology", ["AAPL", "MSFT", "NVDA", "GOOGL", "AMZN", "META", "TSLA",
                    "AMD", "NFLX", "INTC", "ADBE", "CRM", "ORCL", "CSCO",
                    "AVGO", "QCOM", "TXN", "MU", "IBM"]),
    ("Financials", ["JPM", "BAC", "GS", "MS", "WFC", "V", "MA", "AXP"]),
    ("Consumer", ["WMT", "COST", "HD", "MCD", "NKE", "SBUX", "DIS", "KO", "PEP"]),
    ("Healthcare", ["JNJ", "UNH", "PFE", "MRK", "ABBV"]),
    ("Energy & Industrial", ["XOM", "CVX", "BA", "CAT", "F"]),
]


def _ticker_options(current: str) -> str:
    """Build the grouped <option> markup, marking ``current`` as selected.

    A requested ticker outside the curated list is surfaced at the top so the
    dropdown still reflects what is actually displayed.
    """
    current = (current or "").upper()
    known = {t for _, tickers in POPULAR_TICKERS for t in tickers}
    parts = []
    if current and current not in known:
        safe = html.escape(current, quote=True)
        parts.append(f'<option value="{safe}" selected>{safe} · custom</option>')
    for group, tickers in POPULAR_TICKERS:
        parts.append(f'<optgroup label="{group}">')
        for t in tickers:
            sel = " selected" if t == current else ""
            parts.append(f'<option value="{t}"{sel}>{t}</option>')
        parts.append("</optgroup>")
    return "\n".join(parts)


def build_figure(strikes, tenors, vol_surface) -> go.Figure:
    """Build the styled Plotly surface figure (vol expressed as %)."""
    # 1-D x/y with a 2-D z keeps the embedded JSON payload small (Plotly
    # broadcasts them across the grid).
    x = np.asarray(strikes, dtype=float)
    y = np.asarray(tenors, dtype=float)
    Z = np.asarray(vol_surface) * 100.0

    fig = go.Figure(
        data=[
            go.Surface(
                x=x,
                y=y,
                z=Z,
                colorscale=SURFACE_COLORSCALE,
                opacity=0.98,
                lighting=dict(ambient=0.62, diffuse=0.8, specular=0.18, roughness=0.55),
                contours=dict(
                    z=dict(
                        show=True,
                        usecolormap=True,
                        project_z=True,
                        width=1,
                    )
                ),
                colorbar=dict(
                    title=dict(text="IV&nbsp;%", side="right",
                               font=dict(family="IBM Plex Mono, monospace",
                                         size=11, color="#9aa0ad")),
                    tickfont=dict(family="IBM Plex Mono, monospace",
                                  size=10, color="#7d8290"),
                    outlinewidth=0,
                    thickness=12,
                    len=0.62,
                    x=0.99,
                ),
                hovertemplate=(
                    "<b>strike</b> %{x:.2f}<br>"
                    "<b>tenor</b>  %{y:.2f}y<br>"
                    "<b>iv</b>     %{z:.2f}%<extra></extra>"
                ),
            )
        ]
    )

    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=0, r=0, t=0, b=0),
        font=dict(family="IBM Plex Mono, monospace", color="#7d8290"),
        hoverlabel=dict(
            bgcolor="#0f1218",
            bordercolor="#f3b13e",
            font=dict(family="IBM Plex Mono, monospace", size=12, color="#ece8df"),
        ),
        modebar=dict(
            bgcolor="rgba(0,0,0,0)", color="#5c606d", activecolor="#f3b13e",
            orientation="v",
        ),
        scene=dict(
            xaxis=dict(_AXIS, title="STRIKE"),
            yaxis=dict(_AXIS, title="TENOR · Y"),
            zaxis=dict(_AXIS, title="IMPLIED VOL · %"),
            aspectratio=dict(x=1.15, y=1, z=0.72),
            camera=dict(
                eye=dict(x=1.62, y=-1.5, z=0.62),
                up=dict(x=0, y=0, z=1),
            ),
        ),
    )
    return fig


def _nearest(arr: np.ndarray, value: float) -> int:
    return int(np.argmin(np.abs(arr - value)))


def compute_stats(strikes, tenors, vol_surface, spot: float) -> dict:
    """Derive the headline readout from the surface grid.

    Returns a dict of primitives the template can slot in directly; values are
    in display units (%, vol points, years).
    """
    K = np.asarray(strikes, dtype=float)
    T = np.asarray(tenors, dtype=float)
    Z = np.asarray(vol_surface, dtype=float) * 100.0
    spot = float(spot)

    finite = Z[np.isfinite(Z)]
    iv_lo = float(finite.min()) if finite.size else 0.0
    iv_hi = float(finite.max()) if finite.size else 0.0

    # Reference tenor row: closest to ~1 month out (avoids the t=0 slice).
    front_row = _nearest(T, 1.0 / 12.0)
    back_row = len(T) - 1
    atm_col = _nearest(K, spot)
    atm_iv = float(Z[front_row, atm_col])

    # Put skew: 90%-moneyness IV minus 110%-moneyness IV at the front tenor.
    low_col = _nearest(K, 0.9 * spot)
    high_col = _nearest(K, 1.1 * spot)
    skew = float(Z[front_row, low_col] - Z[front_row, high_col])

    # Term structure: back-month ATM minus front-month ATM.
    term = float(Z[back_row, atm_col] - Z[front_row, atm_col])

    return {
        "spot": spot,
        "atm_iv": atm_iv,
        "iv_lo": iv_lo,
        "iv_hi": iv_hi,
        "skew": skew,
        "term": term,
        "tenor_span": float(T.max()),
        "n_strikes": int(K.size),
        "n_nodes": int(Z.size),
    }


# ─────────────────────────────────────────────────────────────────────────────
# HTML template. CSS uses literal braces, so we use string.Template ($name)
# for substitution rather than str.format / f-strings.
# ─────────────────────────────────────────────────────────────────────────────

_PAGE = Template(r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>$ticker · Implied Volatility Surface</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link rel="preconnect" href="https://cdn.plot.ly">
<link rel="dns-prefetch" href="https://cdn.plot.ly">
<link href="https://fonts.googleapis.com/css2?family=Fraunces:opsz,wght@9..144,300..600&family=Hanken+Grotesk:wght@400;500;600&family=IBM+Plex+Mono:wght@400;500;600&display=swap" rel="stylesheet">
<style>
:root{
  --ink:#0a0c11; --ink-2:#0e1117; --ink-3:#141822;
  --line:rgba(236,232,223,.09); --line-2:rgba(236,232,223,.05);
  --text:#ece8df; --muted:#7d8290; --dim:#565b68;
  --gold:#f3b13e; --gold-soft:rgba(243,177,62,.14);
  --peri:#8d9be8; --rose:#ff7a96;
  --serif:"Fraunces",Georgia,serif;
  --mono:"IBM Plex Mono",ui-monospace,monospace;
  --sans:"Hanken Grotesk",system-ui,sans-serif;
}
*{box-sizing:border-box;margin:0;padding:0}
html{-webkit-font-smoothing:antialiased;text-rendering:optimizeLegibility}
body{
  font-family:var(--sans); color:var(--text); background:var(--ink);
  min-height:100vh; overflow-x:hidden; line-height:1.5;
}
::selection{background:var(--gold);color:#0a0c11}
::-webkit-scrollbar{width:10px;height:10px}
::-webkit-scrollbar-track{background:var(--ink)}
::-webkit-scrollbar-thumb{background:#23283400;box-shadow:inset 0 0 0 3px #232834;border-radius:10px}
::-webkit-scrollbar-thumb:hover{box-shadow:inset 0 0 0 3px #343b4c}

/* atmosphere: dual radial glow + grid + grain */
.bg{position:fixed;inset:0;z-index:-2;pointer-events:none;
  background:
    radial-gradient(60vw 60vw at 88% -8%, rgba(243,177,62,.10), transparent 60%),
    radial-gradient(55vw 55vw at -6% 108%, rgba(75,58,143,.22), transparent 60%),
    var(--ink);
}
.grid{position:fixed;inset:0;z-index:-2;pointer-events:none;opacity:.5;
  background-image:
    linear-gradient(var(--line-2) 1px,transparent 1px),
    linear-gradient(90deg,var(--line-2) 1px,transparent 1px);
  background-size:64px 64px;
  -webkit-mask-image:radial-gradient(120vw 120vh at 70% 30%,#000 30%,transparent 85%);
          mask-image:radial-gradient(120vw 120vh at 70% 30%,#000 30%,transparent 85%);
}
.grain{position:fixed;inset:0;z-index:-1;pointer-events:none;opacity:.04;mix-blend-mode:overlay;
  background-image:url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='160' height='160'%3E%3Cfilter id='n'%3E%3CfeTurbulence type='fractalNoise' baseFrequency='.85' numOctaves='3'/%3E%3C/filter%3E%3Crect width='100%25' height='100%25' filter='url(%23n)'/%3E%3C/svg%3E");}

.shell{max-width:1480px;margin:0 auto;padding:clamp(20px,3.4vw,46px);
  min-height:100vh;display:flex;flex-direction:column;gap:clamp(20px,2.6vw,38px)}

/* ── header ── */
header{display:flex;align-items:flex-start;justify-content:space-between;gap:24px;flex-wrap:wrap}
.brand{display:flex;align-items:center;gap:14px}
.mark{width:30px;height:30px;flex:none;position:relative}
.mark svg{display:block;width:100%;height:100%}
.eyebrow{font-family:var(--mono);font-size:11px;letter-spacing:.32em;
  text-transform:uppercase;color:var(--muted)}
.eyebrow b{color:var(--text);font-weight:500}
.eyebrow .sub{display:block;margin-top:3px;color:var(--dim);letter-spacing:.26em;font-size:10px}

form.query{display:flex;align-items:stretch;gap:8px;font-family:var(--mono)}
.field{display:flex;flex-direction:column;gap:5px}
.field label{font-size:9.5px;letter-spacing:.28em;text-transform:uppercase;color:var(--dim);padding-left:2px}
.field input,.field select{
  font-family:var(--mono);font-size:13px;color:var(--text);
  background:var(--ink-2);border:1px solid var(--line);border-radius:9px;
  padding:9px 12px;outline:none;transition:border-color .2s,box-shadow .2s;
}
.field input:focus,.field select:focus{border-color:var(--gold);box-shadow:0 0 0 3px var(--gold-soft)}
.field.tk input,.field.tk select{width:148px;text-transform:uppercase;letter-spacing:.08em;font-weight:500}
.field.dt input{width:160px;color-scheme:dark}
.field select{
  appearance:none;-webkit-appearance:none;cursor:pointer;padding-right:32px;
  background-image:url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='12' height='12' fill='none' stroke='%23f3b13e' stroke-width='2'%3E%3Cpath d='M2 4.5l4 4 4-4' stroke-linecap='round' stroke-linejoin='round'/%3E%3C/svg%3E");
  background-repeat:no-repeat;background-position:right 12px center;
}
.field select option,.field select optgroup{background:var(--ink-2);color:var(--text)}
.field select optgroup{color:var(--muted);font-weight:600;font-style:normal}
.go{align-self:flex-end;font-family:var(--mono);font-size:12px;letter-spacing:.12em;
  text-transform:uppercase;color:#0a0c11;background:var(--gold);border:none;cursor:pointer;
  border-radius:9px;padding:10px 18px;font-weight:600;transition:transform .15s,filter .2s}
.go:hover{filter:brightness(1.08)}
.go:active{transform:translateY(1px)}

/* ── main grid ── */
main{flex:1;display:grid;grid-template-columns:minmax(290px,360px) 1fr;
  gap:clamp(18px,2vw,32px);align-items:stretch}

.rail{display:flex;flex-direction:column;gap:22px;min-width:0}
.ticker-block{position:relative}
.tag{font-family:var(--mono);font-size:10.5px;letter-spacing:.28em;text-transform:uppercase;
  color:var(--gold);display:flex;align-items:center;gap:8px}
.tag::before{content:"";width:7px;height:7px;border-radius:50%;background:var(--gold);
  box-shadow:0 0 12px var(--gold)}
.ticker{font-family:var(--serif);font-weight:380;font-size:clamp(58px,8.6vw,108px);
  line-height:.86;letter-spacing:-.02em;margin:14px 0 0;color:var(--text);
  position:relative;display:inline-block}
.ticker::after{content:"";position:absolute;left:2px;right:0;bottom:-10px;height:3px;
  background:linear-gradient(90deg,var(--gold),transparent);
  transform:scaleX(0);transform-origin:left;animation:draw .9s .5s cubic-bezier(.2,.8,.2,1) forwards}
.subtitle{font-family:var(--serif);font-style:italic;font-size:18px;color:var(--muted);
  margin-top:20px;font-weight:300}

.readout{display:grid;grid-template-columns:1fr 1fr;gap:1px;
  background:var(--line);border:1px solid var(--line);border-radius:14px;overflow:hidden}
.stat{background:var(--ink-2);padding:15px 16px 16px;display:flex;flex-direction:column;gap:7px}
.stat .k{font-family:var(--mono);font-size:9.5px;letter-spacing:.2em;text-transform:uppercase;color:var(--dim)}
.stat .v{font-family:var(--mono);font-size:25px;font-weight:500;color:var(--text);
  line-height:1;font-variant-numeric:tabular-nums;display:flex;align-items:baseline;gap:3px}
.stat .v .u{font-size:13px;color:var(--muted);font-weight:400}
.stat .v.pos{color:var(--gold)} .stat .v.neg{color:var(--peri)}
.stat.wide{grid-column:1/-1}

.note{font-size:12.5px;color:var(--dim);line-height:1.6;border-left:2px solid var(--line);padding-left:14px}
.note b{color:var(--muted);font-weight:600}

/* ── surface panel ── */
.panel{position:relative;background:linear-gradient(180deg,var(--ink-2),#0b0e14);
  border:1px solid var(--line);border-radius:18px;overflow:hidden;
  display:flex;flex-direction:column;min-height:520px;
  box-shadow:0 40px 90px -50px rgba(0,0,0,.9),inset 0 1px 0 rgba(236,232,223,.04)}
.panel-bar{display:flex;align-items:center;justify-content:space-between;
  padding:14px 20px;border-bottom:1px solid var(--line);
  font-family:var(--mono);font-size:10.5px;letter-spacing:.2em;text-transform:uppercase;color:var(--muted)}
.panel-bar .hint{color:var(--dim);display:flex;align-items:center;gap:7px}
.panel-bar .hint svg{opacity:.6}
.plot-wrap{position:relative;flex:1;min-height:440px}
.plot-wrap > div{position:absolute;inset:0}
#figure-data{display:none}
.plot-skel{display:flex;flex-direction:column;align-items:center;justify-content:center;gap:20px;
  background:radial-gradient(58% 58% at 50% 44%,rgba(141,155,232,.07),transparent 72%);
  font-family:var(--mono);font-size:10.5px;letter-spacing:.26em;text-transform:uppercase;color:var(--muted);
  transition:opacity .55s ease}
.plot-skel.hide{opacity:0;pointer-events:none}
.skel-orbit{width:46px;height:46px;border-radius:50%;
  border:2px solid var(--line);border-top-color:var(--gold);border-right-color:var(--peri);
  animation:spin 1s linear infinite}
.skel-grid{position:absolute;inset:0;opacity:.4;
  background-image:linear-gradient(var(--line-2) 1px,transparent 1px),
    linear-gradient(90deg,var(--line-2) 1px,transparent 1px);
  background-size:40px 40px;
  -webkit-mask-image:radial-gradient(70% 70% at 50% 50%,#000,transparent 78%);
          mask-image:radial-gradient(70% 70% at 50% 50%,#000,transparent 78%)}
@keyframes spin{to{transform:rotate(360deg)}}
.scale{display:flex;align-items:center;gap:13px;padding:13px 20px;border-top:1px solid var(--line);
  font-family:var(--mono);font-size:10px;letter-spacing:.16em;text-transform:uppercase;color:var(--dim)}
.scale .bar{flex:1;height:6px;border-radius:6px;
  background:linear-gradient(90deg,#1b1f47,#4b3a8f,#8e3f9e,#e0653f,#f3b13e,#ffeec2)}

footer{display:flex;justify-content:space-between;align-items:center;gap:16px;flex-wrap:wrap;
  font-family:var(--mono);font-size:10.5px;letter-spacing:.16em;text-transform:uppercase;color:var(--dim);
  border-top:1px solid var(--line);padding-top:18px}
footer a{color:var(--muted);text-decoration:none;border-bottom:1px solid var(--line)}
footer a:hover{color:var(--gold);border-color:var(--gold)}

.method{border-top:1px solid var(--line);padding-top:clamp(30px,4vw,54px);
  display:flex;flex-direction:column;gap:clamp(24px,3vw,36px)}
.method-head{max-width:720px}
.method-head .eyebrow{color:var(--gold)}
.method-head h2{font-family:var(--serif);font-weight:380;letter-spacing:-.01em;
  font-size:clamp(28px,4vw,44px);line-height:1.05;margin:14px 0 0}
.method-head .lead{font-size:16px;color:var(--muted);margin-top:16px;line-height:1.62}
.method-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:1px;
  background:var(--line);border:1px solid var(--line);border-radius:16px;overflow:hidden}
.method-grid article{background:var(--ink-2);padding:26px 24px;display:flex;flex-direction:column;gap:12px}
.method-grid article:hover{background:var(--ink-3)}
.num-tag{font-family:var(--mono);font-size:10px;letter-spacing:.18em;text-transform:uppercase;color:var(--gold)}
.method-grid h3{font-family:var(--serif);font-weight:400;font-size:20px;color:var(--text);line-height:1.15}
.method-grid p{font-size:13.5px;color:var(--muted);line-height:1.64}
.method-grid b{color:var(--text);font-weight:600}
.method-foot{font-family:var(--mono);font-size:11px;letter-spacing:.03em;color:var(--dim);
  line-height:1.6;border-left:2px solid var(--line);padding-left:14px;max-width:720px}

@keyframes rise{from{opacity:0;transform:translateY(16px)}to{opacity:1;transform:none}}
@keyframes draw{to{transform:scaleX(1)}}
.rise{opacity:0;animation:rise .8s cubic-bezier(.2,.8,.2,1) forwards}
$delays
@media (prefers-reduced-motion:reduce){
  .rise,.ticker::after,.tag::before{animation:none!important;opacity:1!important;transform:none!important}
}
@media (max-width:920px){
  main{grid-template-columns:1fr}
  .method-grid{grid-template-columns:1fr}
  .panel{min-height:460px}
  header{flex-direction:column}
}
</style>
</head>
<body>
<div class="bg"></div><div class="grid"></div><div class="grain"></div>

<div class="shell">
  <header class="rise" style="--d:.02s">
    <div class="brand">
      <span class="mark">
        <svg viewBox="0 0 40 40" fill="none">
          <path d="M3 30 C11 30 11 12 20 12 C29 12 29 26 37 26" stroke="#f3b13e" stroke-width="2.2" stroke-linecap="round"/>
          <path d="M3 35 C11 35 11 20 20 20 C29 20 29 31 37 31" stroke="#8d9be8" stroke-width="1.6" stroke-linecap="round" opacity=".7"/>
          <circle cx="20" cy="12" r="2.4" fill="#f3b13e"/>
        </svg>
      </span>
      <span class="eyebrow"><b>VOLΣ</b> · IMPLIED VOLATILITY SURFACE
        <span class="sub">CBOE OPTIONS · QUANTLIB BLACK-VARIANCE</span></span>
    </div>
    <form class="query" method="get" action="">
      <div class="field tk">
        <label for="t">Ticker</label>
        <select id="t" name="ticker" onchange="this.form.submit()">
$ticker_options
        </select>
      </div>
      <div class="field dt">
        <label for="d">As of</label>
        <input id="d" name="date" type="date" value="$date">
      </div>
      <button class="go" type="submit">Build →</button>
    </form>
  </header>

  <main>
    <aside class="rail">
      <div class="ticker-block rise" style="--d:.10s">
        <div class="tag">As of · $date</div>
        <h1 class="ticker">$ticker</h1>
        <p class="subtitle">Implied volatility across strike &amp; tenor.</p>
      </div>

      <div class="readout rise" style="--d:.18s">
        <div class="stat"><span class="k">Spot</span>
          <span class="v"><span class="u">$$</span><span class="num" data-to="$spot" data-dp="2">0</span></span></div>
        <div class="stat"><span class="k">ATM IV · 1M</span>
          <span class="v"><span class="num" data-to="$atm_iv" data-dp="1">0</span><span class="u">%</span></span></div>
        <div class="stat"><span class="k">Skew · 90/110</span>
          <span class="v $skew_cls"><span class="num" data-to="$skew" data-dp="1" data-sign="1">0</span><span class="u">pts</span></span></div>
        <div class="stat"><span class="k">Term · F→B</span>
          <span class="v $term_cls"><span class="num" data-to="$term" data-dp="1" data-sign="1">0</span><span class="u">pts</span></span></div>
        <div class="stat wide"><span class="k">IV Range</span>
          <span class="v"><span class="num" data-to="$iv_lo" data-dp="1">0</span><span class="u">%</span>
            <span class="u" style="padding:0 4px">—</span>
            <span class="num" data-to="$iv_hi" data-dp="1">0</span><span class="u">%</span></span></div>
        <div class="stat"><span class="k">Tenor span</span>
          <span class="v"><span class="num" data-to="$tenor_span" data-dp="2">0</span><span class="u">y</span></span></div>
        <div class="stat"><span class="k">Strikes</span>
          <span class="v"><span class="num" data-to="$n_strikes" data-dp="0">0</span></span></div>
      </div>

      <p class="note rise" style="--d:.26s">Vols solved from bid/ask mid via a
        <b>Black–Scholes–Merton</b> model, then fitted to a <b>Black-variance
        surface</b>. Flat assumptions: <b>r&nbsp;3%</b>, <b>q&nbsp;1%</b>.
        Grid resolved to <b>$n_nodes</b> nodes.</p>
    </aside>

    <section class="panel rise" style="--d:.20s">
      <div class="panel-bar">
        <span>3D · $ticker Surface</span>
        <span class="hint">
          <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
            <path d="M12 3v3m0 12v3M3 12h3m12 0h3" stroke-linecap="round"/><circle cx="12" cy="12" r="4"/></svg>
          Drag to rotate · scroll to zoom</span>
      </div>
      <div class="plot-wrap">
        <div id="surface"></div>
        <div class="plot-skel" id="plot-skel">
          <div class="skel-grid"></div>
          <div class="skel-orbit"></div>
          <span>Rendering surface</span>
        </div>
        <script type="application/json" id="figure-data">$figure_json</script>
      </div>
      <div class="scale">
        <span>Calm</span><span class="bar"></span><span>Stressed</span>
      </div>
    </section>
  </main>

  <section class="method rise" style="--d:.40s">
    <div class="method-head">
      <span class="eyebrow">Methodology</span>
      <h2>Reading the volatility surface</h2>
      <p class="lead">An implied volatility surface is the options market's map
        of risk for a single underlying — one volatility number for every strike
        and expiry, drawn as a landscape rather than a single line. Its shape
        encodes how the market prices uncertainty across price and time.</p>
    </div>
    <div class="method-grid">
      <article>
        <span class="num-tag">01 · What the axes show</span>
        <h3>Strike, tenor, volatility</h3>
        <p><b>Strike</b> (x) is the option's exercise price; <b>tenor</b> (y) is
          time to expiry in years; <b>height &amp; colour</b> (z) are annualised
          implied volatility — cool indigo is calm, warm amber is stressed. The
          tilt across strikes is the <b>skew / smile</b>; the slope across
          tenors is the <b>term structure</b>.</p>
      </article>
      <article>
        <span class="num-tag">02 · How it's built</span>
        <h3>From quotes to a surface</h3>
        <p>For each near-the-money option we invert the <b>bid/ask mid</b> price
          through a Black–Scholes–Merton model (QuantLib) to recover its implied
          vol, assuming flat <b>r&nbsp;3%</b> and <b>q&nbsp;1%</b>. Those points
          are fitted into a QuantLib <b>Black-variance surface</b>, then sampled
          on a fine strike&nbsp;×&nbsp;tenor grid.</p>
      </article>
      <article>
        <span class="num-tag">03 · What it's useful for</span>
        <h3>Pricing, risk &amp; relative value</h3>
        <p>Price and hedge options consistently across the whole chain, spot
          <b>rich / cheap</b> strikes and expiries, gauge how the market prices
          crash risk via the <b>put skew</b>, and read its expectation of
          near- versus long-term turbulence from the <b>term structure</b>.</p>
      </article>
    </div>
    <p class="method-foot">Figures are model-derived from delayed CBOE quotes
      with flat rate and dividend assumptions — illustrative, not
      trading-grade marks.</p>
  </section>

  <footer class="rise" style="--d:.46s">
    <span>Source · CBOE delayed options · rendered $date</span>
    <span>Azure Functions · <a href="https://github.com/an21p/volatility-surface-azure">an21p/volatility-surface-azure</a></span>
  </footer>
</div>

<script>
// count-up reveal for the readout
(function(){
  var reduce = matchMedia('(prefers-reduced-motion:reduce)').matches;
  document.querySelectorAll('.num').forEach(function(el){
    var to=parseFloat(el.dataset.to)||0, dp=parseInt(el.dataset.dp)||0,
        sign=el.dataset.sign==='1';
    var fmt=function(v){ var s=v.toLocaleString('en-US',
        {minimumFractionDigits:dp,maximumFractionDigits:dp});
        return (sign&&v>0?'+':'')+s; };
    if(reduce){ el.textContent=fmt(to); return; }
    var dur=900, t0=null;
    function step(t){ if(!t0)t0=t; var p=Math.min((t-t0)/dur,1);
      var e=1-Math.pow(1-p,3); el.textContent=fmt(to*e);
      if(p<1)requestAnimationFrame(step); else el.textContent=fmt(to); }
    requestAnimationFrame(step);
  });
})();

// Lazy-load Plotly only after the page (chrome + readout) has painted, then
// draw the surface into its skeleton. The 3D renderer is the heavy part, so we
// defer it to idle time instead of blocking first paint.
(function(){
  var el = document.getElementById('surface'),
      skel = document.getElementById('plot-skel'),
      src = document.getElementById('figure-data');
  if(!el || !src) return;
  var fig;
  try { fig = JSON.parse(src.textContent); } catch(e){ return; }

  function draw(){
    Plotly.newPlot(el, fig.data, fig.layout, {
      responsive:true, displaylogo:false, scrollZoom:true,
      modeBarButtonsToRemove:['resetCameraLastSave3d']
    }).then(function(){ if(skel) skel.classList.add('hide'); });
  }
  function load(){
    if(window.Plotly){ draw(); return; }
    var s = document.createElement('script');
    s.src = "$plotly_cdn"; s.async = true;
    s.onload = draw;
    s.onerror = function(){ if(skel) skel.innerHTML =
      '<span>Renderer failed to load · reload to retry</span>'; };
    document.head.appendChild(s);
  }
  if('requestIdleCallback' in window){ requestIdleCallback(load, {timeout:1400}); }
  else { setTimeout(load, 200); }
})();
</script>
</body>
</html>
""")


def render_page(ticker: str, date_str: str, figure_json: str, stats: dict) -> str:
    """Assemble the full HTML page from a figure JSON blob and the readout.

    The figure is embedded as JSON (not a pre-rendered Plotly div) so the page
    paints immediately and Plotly.js is fetched lazily, client-side.
    """
    return _PAGE.safe_substitute(
        ticker=ticker,
        date=date_str,
        ticker_options=_ticker_options(ticker),
        figure_json=figure_json,
        plotly_cdn=PLOTLY_CDN,
        delays="",  # reserved hook; per-element delays set inline via --d
        spot=f"{stats['spot']:.2f}",
        atm_iv=f"{stats['atm_iv']:.1f}",
        iv_lo=f"{stats['iv_lo']:.1f}",
        iv_hi=f"{stats['iv_hi']:.1f}",
        skew=f"{stats['skew']:.1f}",
        term=f"{stats['term']:.1f}",
        skew_cls="pos" if stats["skew"] >= 0 else "neg",
        term_cls="pos" if stats["term"] >= 0 else "neg",
        tenor_span=f"{stats['tenor_span']:.2f}",
        n_strikes=str(stats["n_strikes"]),
        n_nodes=f"{stats['n_nodes']:,}",
    )


def render_surface_html(ticker: str, date_str: str, strikes, tenors, vol_surface,
                        spot: Optional[float] = None) -> str:
    """End-to-end: build the figure, derive stats, return a full HTML page."""
    if spot is None:
        spot = float(np.asarray(strikes, dtype=float).mean())

    fig = build_figure(strikes, tenors, vol_surface)
    # pio.to_json handles numpy arrays and emits a compact figure blob the
    # client parses and hands to Plotly.newPlot once the library has loaded.
    figure_json = pio.to_json(fig)
    stats = compute_stats(strikes, tenors, vol_surface, spot)
    return render_page(ticker, date_str, figure_json, stats)
