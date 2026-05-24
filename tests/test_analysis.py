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


from unittest.mock import MagicMock


def _fake_response(status=200, content="ok"):
    r = MagicMock()
    r.status_code = status
    r.json.return_value = {"choices": [{"message": {"content": content}}]}
    r.text = content
    return r


def test_call_openai_missing_key(monkeypatch):
    monkeypatch.delenv("OPEN_AI_API", raising=False)
    assert analysis._call_openai([{"role": "user", "content": "hi"}]) is None


def test_call_openai_success_strips_dashes(monkeypatch):
    monkeypatch.setenv("OPEN_AI_API", "sk-test")
    monkeypatch.setattr(analysis.requests, "post",
                        lambda *a, **k: _fake_response(200, "Vol is high — skew steep. "))
    out = analysis._call_openai([{"role": "user", "content": "hi"}])
    assert out == "Vol is high - skew steep."   # trimmed + em-dash converted


def test_call_openai_non_200(monkeypatch):
    monkeypatch.setenv("OPEN_AI_API", "sk-test")
    monkeypatch.setattr(analysis.requests, "post", lambda *a, **k: _fake_response(500, "boom"))
    assert analysis._call_openai([{"role": "user", "content": "hi"}]) is None


def test_call_openai_exception(monkeypatch):
    monkeypatch.setenv("OPEN_AI_API", "sk-test")
    def boom(*a, **k):
        raise RuntimeError("network")
    monkeypatch.setattr(analysis.requests, "post", boom)
    assert analysis._call_openai([{"role": "user", "content": "hi"}]) is None
