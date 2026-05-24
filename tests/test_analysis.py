import numpy as np
from conftest import load_module

analysis = load_module("volatility_surface/analysis.py", "analysis")


def test_coarse_grid_downsamples_and_scales():
    strikes = np.linspace(100, 200, 21)
    tenors = np.linspace(0.0, 1.5, 50)
    vol = np.full((50, 21), 0.20)  # 20% in decimal
    g = analysis.coarse_grid(strikes, tenors, vol, n=5)
    assert len(g["strikes"]) == 5
    assert len(g["tenors"]) == 5
    assert len(g["iv"]) == 5 and len(g["iv"][0]) == 5
    assert g["iv"][0][0] == 20.0          # scaled to percent
    assert g["strikes"][0] == 100.0 and g["strikes"][-1] == 200.0


import json as _json


def _stats():
    return {"spot": 242.0, "atm_iv": 37.0, "skew": 8.1, "term": 9.2,
            "iv_lo": 30.0, "iv_hi": 111.7, "tenor_span": 1.5,
            "n_strikes": 21, "n_nodes": 1050}


def test_build_prompt_structure():
    grid = {"strikes": [200, 242, 284], "tenors": [0.1, 1.5], "iv": [[40, 37, 41], [45, 42, 46]]}
    msgs = analysis.build_prompt("TSLA", "2026-05-24", _stats(), grid)
    assert msgs[0]["role"] == "system"
    assert "dash" in msgs[0]["content"].lower()      # instructs no dashes
    assert msgs[1]["role"] == "user"
    payload = _json.loads(msgs[1]["content"])         # user content is JSON
    assert payload["ticker"] == "TSLA"
    assert payload["atm_iv_pct"] == 37.0
    assert payload["coarse_iv_grid"]["strikes"] == [200, 242, 284]
