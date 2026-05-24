"""Local preview server for the volatility-surface page.

Serves the SAME page the Azure endpoint renders. By default it's backed by a
synthetic surface (deterministic per ticker) so you can test the design and the
ticker/date form without Azure Storage, CBOE, or QuantLib.

    python scripts/serve_preview.py            # -> http://localhost:8000
    python scripts/serve_preview.py --port 9000

Pass --data <chain.csv> to render the REAL pipeline (build_surface + SSVI) on an
option chain instead. That path needs QuantLib + scipy (e.g. the .devvenv):

    .devvenv/bin/python scripts/serve_preview.py --data tests/fixtures/options_sample.csv

Routes: "/" and "/api/volatility-surface" both accept ?ticker= & ?date=.
"""
import argparse
import importlib.util
import os
import zlib
from datetime import date as _date
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

import numpy as np

# Load surface_page.py directly (avoid the package __init__'s azure imports).
_HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_spec = importlib.util.spec_from_file_location(
    "surface_page", os.path.join(_HERE, "volatility_surface", "surface_page.py"))
_sp = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_sp)


def synthetic_surface(ticker: str, n_strikes=21, n_tenors=50):
    """Stable, ticker-varied surface so each symbol looks distinct."""
    seed = zlib.crc32(ticker.encode()) & 0xFFFFFFFF
    rng = np.random.default_rng(seed)
    spot = float(rng.uniform(40, 480))
    base_vol = float(rng.uniform(0.18, 0.55))      # ATM level
    skew = float(rng.uniform(0.3, 0.9))            # put-skew strength
    term = float(rng.uniform(0.06, 0.16))          # term-structure slope

    strikes = np.linspace(0.6 * spot, 1.4 * spot, n_strikes)
    tenors = np.linspace(0.0, 1.5, n_tenors)
    moneyness = np.log(strikes / spot)

    vol = np.zeros((n_tenors, n_strikes))
    for i, t in enumerate(tenors):
        atm = base_vol + term * np.sqrt(t)
        smile = 1.9 * moneyness ** 2 - skew * moneyness
        vol[i] = atm + smile / (1.0 + 2.2 * t)
    return strikes, tenors, np.clip(vol, 0.05, 1.6), spot


_REAL_CACHE = {}


def real_surface(data_path: str):
    """Run the actual build_surface (QuantLib + SSVI) on an option-chain CSV.

    Cached so the page reloads quickly; the surface is the same regardless of
    the ?ticker= query (which only sets the page title)."""
    if "v" not in _REAL_CACHE:
        import pandas as pd
        import sys
        sys.path.insert(0, _HERE)
        import visualiser
        df = pd.read_csv(data_path)
        strikes, tenors, vol = visualiser.build_surface(df)
        _REAL_CACHE["v"] = (strikes, tenors, vol, float(df["spot"].iloc[0]))
    return _REAL_CACHE["v"]


class Handler(BaseHTTPRequestHandler):
    data_path = None  # set by main() when --data is given


    def log_message(self, fmt, *args):
        print(f"  {self.address_string()} - {fmt % args}")

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path not in ("/", "/api/volatility-surface", "/volatility-surface",
                                "/api/surface-analysis"):
            self.send_error(404, "Not found")
            return

        if parsed.path == "/api/surface-analysis":
            import json as _json
            qs = parse_qs(parsed.query)
            tk = qs.get("ticker", ["SPY"])[0].upper()
            body = _json.dumps({
                "analysis": (f"{tk} shows an at-the-money implied vol around 37%, "
                             "with a clear downside skew and a gently upward term "
                             "structure. The wings lift toward both low and high "
                             "strikes, a typical equity smile. (preview stub)"),
                "cached": False,
            }).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        qs = parse_qs(parsed.query)
        ticker = qs.get("ticker", ["SPY"])[0].strip().upper() or "SPY"
        date_str = qs.get("date", [_date.today().isoformat()])[0]

        if self.data_path:
            strikes, tenors, vol, spot = real_surface(self.data_path)
        else:
            strikes, tenors, vol, spot = synthetic_surface(ticker)
        html = _sp.render_surface_html(ticker, date_str, strikes, tenors, vol, spot)
        body = html.encode("utf-8")

        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--port", type=int, default=8000)
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--data", default=None,
                   help="option-chain CSV to render via the real build_surface "
                        "(needs QuantLib + scipy)")
    a = p.parse_args()

    Handler.data_path = a.data

    srv = ThreadingHTTPServer((a.host, a.port), Handler)
    url = f"http://{a.host}:{a.port}/"
    src = f"real pipeline · {a.data}" if a.data else "synthetic data"
    print(f"\n  Volatility surface preview  →  {url}")
    print(f"  Try: {url}?ticker=NVDA   ·   {url}?ticker=AAPL&date=2026-05-24")
    print(f"  ({src} · Ctrl-C to stop)\n")
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        srv.shutdown()


if __name__ == "__main__":
    main()
