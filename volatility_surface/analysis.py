"""AI analysis of the implied-volatility surface.

Pure logic (no Azure types): downsample the surface, build the chat prompt,
and call the OpenAI chat completions API. The HTTP route in __init__ wires
this to blob caching.
"""

import json
import os
from logging import warning
from typing import Optional

import numpy as np
import requests
from dotenv import load_dotenv

load_dotenv()  # local dev: read OPEN_AI_API from .env (no-op in Azure)

OPENAI_URL = "https://api.openai.com/v1/chat/completions"
MODEL = "gpt-4o-mini"
_DASHES = str.maketrans({"—": "-", "–": "-"})


def coarse_grid(strikes, tenors, vol_surface, n: int = 5) -> dict:
    """Downsample the surface to ~n x n, IV expressed as %."""
    strikes = np.asarray(strikes, dtype=float)
    tenors = np.asarray(tenors, dtype=float)
    Z = np.asarray(vol_surface, dtype=float) * 100.0
    si = np.unique(np.linspace(0, len(strikes) - 1,
                               min(n, len(strikes))).round().astype(int))
    ti = np.unique(np.linspace(0, len(tenors) - 1,
                               min(n, len(tenors))).round().astype(int))
    return {
        "strikes": [round(float(x), 2) for x in strikes[si]],
        "tenors": [round(float(t), 3) for t in tenors[ti]],
        "iv": [[round(float(Z[i, j]), 2) for j in si] for i in ti],
    }


SYSTEM_PROMPT = (
    "You are a quantitative analyst. Given summary statistics and a coarse grid "
    "of an equity option implied-volatility surface, write a concise, plain-English "
    "explanation of what it shows: the overall volatility level, the skew across "
    "strikes, the term structure across expiries, and any notable feature. Write 2 "
    "to 3 sentences, about 60 words. Be specific and use percentages. Do not give "
    "financial advice. Do not use dashes of any kind; write in plain sentences."
)


def build_prompt(ticker: str, date_str: str, stats: dict, grid: dict) -> list:
    payload = {
        "ticker": ticker,
        "as_of": date_str,
        "spot": round(stats["spot"], 2),
        "atm_iv_pct": round(stats["atm_iv"], 1),
        "skew_90_110_pts": round(stats["skew"], 1),
        "term_front_to_back_pts": round(stats["term"], 1),
        "iv_low_pct": round(stats["iv_lo"], 1),
        "iv_high_pct": round(stats["iv_hi"], 1),
        "tenor_span_years": round(stats["tenor_span"], 2),
        "n_strikes": stats["n_strikes"],
        "coarse_iv_grid": grid,
    }
    return [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": json.dumps(payload)},
    ]


def _call_openai(messages, timeout: int = 15) -> Optional[str]:
    key = os.environ.get("OPEN_AI_API")
    if not key:
        warning("analysis: OPEN_AI_API not set; skipping analysis")
        return None
    try:
        resp = requests.post(
            OPENAI_URL,
            headers={"Authorization": f"Bearer {key}",
                     "Content-Type": "application/json"},
            json={"model": MODEL, "messages": messages,
                  "max_tokens": 160, "temperature": 0.4},
            timeout=timeout,
        )
        if resp.status_code != 200:
            warning(f"analysis: OpenAI {resp.status_code}: {resp.text[:200]}")
            return None
        text = resp.json()["choices"][0]["message"]["content"].strip()
        return text.translate(_DASHES) or None
    except Exception as e:  # noqa: BLE001 - never break the page on analysis
        warning(f"analysis: OpenAI call failed: {e}")
        return None


def analyse_surface(ticker: str, date_str: str, stats: dict, grid: dict) -> Optional[str]:
    """Build the prompt and return the model's text, or None on any failure."""
    return _call_openai(build_prompt(ticker, date_str, stats, grid))
