"""Synthetic option-chain generator for tests and the local preview.

Prices European options under a *known* SSVI smile so the full pipeline
(``build_surface`` re-solving IV from bid/ask mids and re-fitting SSVI) can be
checked against ground truth. Uses an OTM blend (calls for K>=spot, puts below)
to mirror the production filter.
"""
from __future__ import annotations

from typing import Sequence

import numpy as np
import pandas as pd
from scipy.special import ndtr  # standard normal CDF

from visualiser.ssvi import ssvi_total_variance


def _bs_price(S: float, K: float, t: float, sigma: float, r: float, q: float,
              is_call: bool) -> float:
    sqt = sigma * np.sqrt(t)
    d1 = (np.log(S / K) + (r - q + 0.5 * sigma ** 2) * t) / sqt
    d2 = d1 - sqt
    if is_call:
        return S * np.exp(-q * t) * ndtr(d1) - K * np.exp(-r * t) * ndtr(d2)
    return K * np.exp(-r * t) * ndtr(-d2) - S * np.exp(-q * t) * ndtr(-d1)


def synth_option_chain(expiries: Sequence[str], today: str, spot: float = 100.0,
                       n_strikes: int = 21, lo: float = 0.70, hi: float = 1.30,
                       rho: float = -0.4, eta: float = 1.2, gamma: float = 0.35,
                       sigma_atm: float = 0.22, r: float = 0.03, q: float = 0.01,
                       spread: float = 0.02) -> pd.DataFrame:
    """Build an option chain (expiry, spot, strike, bid, ask, iv, type).

    ``expiries`` and ``today`` are ISO date strings; each option is priced at the
    SSVI implied vol for its (tenor, log-moneyness) and quoted with a small
    relative bid/ask spread.
    """
    strikes = np.round(np.linspace(lo * spot, hi * spot, n_strikes), 2)
    rows = []
    today_ts = pd.Timestamp(today)
    for exp in expiries:
        t = (pd.Timestamp(exp) - today_ts).days / 365.0
        if t <= 0:
            continue
        theta = sigma_atm ** 2 * t
        fwd = spot * np.exp((r - q) * t)
        for K in strikes:
            k = float(np.log(K / fwd))
            sigma = float(np.sqrt(ssvi_total_variance(k, theta, rho, eta, gamma) / t))
            is_call = K >= spot
            price = _bs_price(spot, float(K), t, sigma, r, q, is_call)
            half = max(spread * price, 0.01)
            rows.append({
                "expiry": pd.Timestamp(exp).strftime("%Y-%m-%d"),
                "spot": spot,
                "strike": float(K),
                "bid": round(price - half, 4),
                "ask": round(price + half, 4),
                "iv": round(sigma, 4),
                "type": "C" if is_call else "P",
            })
    return pd.DataFrame(rows)
