"""Tests for the option-chain filter (OTM blend across a moneyness band)."""
import pandas as pd
import pytest

import utils


class _StubContainer:
    """Captures upload_blob calls instead of talking to Azure."""
    def __init__(self):
        self.uploaded = {}

    def upload_blob(self, name, data, overwrite=False):
        self.uploaded[name] = data


def _raw_chain():
    today = pd.Timestamp.today().normalize()
    candidates = pd.date_range(today, today + pd.Timedelta(days=160))
    third_fridays = [d for d in candidates if utils.is_third_friday(d)]
    assert third_fridays, "expected an upcoming third-Friday in the window"
    expiries = third_fridays[:2]

    spot = 100.0
    rows = []
    for exp in expiries:
        for strike in range(50, 161, 5):  # 50%..160% of spot, both wings + ITM
            for opt in ("C", "P"):
                rows.append({"expiry": exp, "spot": spot, "strike": float(strike),
                             "bid": 1.0, "ask": 1.2, "iv": 0.25, "type": opt})
    return pd.DataFrame(rows), spot, expiries


def test_filter_keeps_otm_blend_within_band():
    raw, spot, expiries = _raw_chain()
    out = utils.upload_filtered_options("X", _StubContainer(), raw, "blob.csv")

    assert out is not None and not out.empty
    # Within the moneyness band.
    assert (out["strike"] >= utils.MONEYNESS_LO * spot).all()
    assert (out["strike"] <= utils.MONEYNESS_HI * spot).all()
    # OTM blend: puts strictly below spot, calls at/above spot.
    assert (out.loc[out["strike"] < spot, "type"] == "P").all()
    assert (out.loc[out["strike"] >= spot, "type"] == "C").all()
    # One option per (expiry, strike) — the OTM side only.
    assert not out.duplicated(subset=["expiry", "strike"]).any()
    # Keeps the full term structure (all upcoming standard expiries).
    assert out["expiry"].nunique() == len(expiries)
    # Per-expiry width is capped.
    assert out.groupby("expiry").size().max() <= utils.MAX_STRIKES_PER_EXPIRY
