"""Render a standalone preview of the volatility-surface page with synthetic
data (no Azure / CBOE / QuantLib needed). Reuses the real presentation module.

    python scripts/preview_surface.py --ticker TSLA --out preview.html
"""
import argparse
import importlib.util
import os

import numpy as np

# Load surface_page.py directly so we don't trigger the package __init__
# (which imports azure / utils that aren't needed for a static preview).
_HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_spec = importlib.util.spec_from_file_location(
    "surface_page", os.path.join(_HERE, "volatility_surface", "surface_page.py"))
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)
render_surface_html = _mod.render_surface_html


def synthetic_surface(spot=242.0, n_strikes=21, n_tenors=50):
    """A plausible equity vol surface: smile across strike, term structure in
    tenor, mild upward drift — shaped like the real QuantLib output."""
    strikes = np.linspace(0.6 * spot, 1.4 * spot, n_strikes)
    tenors = np.linspace(0.0, 1.5, n_tenors)

    moneyness = np.log(strikes / spot)
    vol = np.zeros((n_tenors, n_strikes))
    for i, t in enumerate(tenors):
        atm = 0.34 + 0.10 * np.sqrt(t)                 # term structure
        smile = 1.9 * moneyness ** 2 - 0.55 * moneyness  # skewed smile
        vol[i] = atm + smile / (1.0 + 2.2 * t)
    return strikes, tenors, np.clip(vol, 0.08, 1.5), spot


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--ticker", default="TSLA")
    p.add_argument("--date", default="2026-05-24")
    p.add_argument("--out", default="preview.html")
    a = p.parse_args()

    strikes, tenors, vol, spot = synthetic_surface()
    html = render_surface_html(a.ticker.upper(), a.date, strikes, tenors, vol, spot)
    with open(a.out, "w") as f:
        f.write(html)
    print(f"wrote {a.out} ({len(html):,} bytes)")


if __name__ == "__main__":
    main()
