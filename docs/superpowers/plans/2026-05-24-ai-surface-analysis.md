# AI Surface Analysis Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Show a short, AI-generated explanation of the rendered volatility surface below the chart header, fetched lazily and cached per ticker+date.

**Architecture:** A new anonymous HTTP endpoint `/api/surface-analysis` builds the surface, derives stats + a coarse IV grid, asks `gpt-4o-mini` to explain it, and caches the text in blob storage. The surface page fetches it client-side after load and renders it (or hides it on failure). Pure analysis logic lives in `volatility_surface/analysis.py` and is unit-tested; the Azure glue and UI are verified manually.

**Tech Stack:** Python 3.12, Azure Functions (Python v2 model), `requests` (OpenAI REST), `python-dotenv`, pytest, Plotly (existing page).

---

## File Structure

- `volatility_surface/analysis.py` (new) — `coarse_grid`, `build_prompt`, `_call_openai`, `analyse_surface`. No Azure types.
- `utils/__init__.py` (modify) — `get_analysis_blob_name`, `read_text_blob`, `write_text_blob`.
- `volatility_surface/__init__.py` (modify) — new `analysis` blueprint + `surface-analysis` route.
- `function_app.py` (modify) — register the blueprint.
- `volatility_surface/surface_page.py` (modify) — UI block below `.panel-bar`, CSS, fetch JS.
- `scripts/serve_preview.py` (modify) — stub `/api/surface-analysis` so the UI is previewable locally.
- `requirements.txt` (modify) — add `python-dotenv`.
- `.env.example` (new), `README.md` / `CLAUDE.md` (modify) — docs.
- `tests/conftest.py`, `tests/test_analysis.py`, `tests/test_utils_blobs.py` (new).

Tests run in the existing `.preview-venv`. Test imports load single module files directly (via importlib) to avoid the package `__init__`'s `azure.functions` import.

---

### Task 1: Test infra + scaffolding

**Files:**
- Create: `.env.example`, `tests/conftest.py`
- Modify: `requirements.txt`

- [ ] **Step 1: Add `python-dotenv` to requirements**

Add this line to `requirements.txt` (keep alphabetical-ish grouping; place after `pyparsing`):

```
python-dotenv==1.0.1
```

- [ ] **Step 2: Create `.env.example`**

```
# OpenAI key used for the AI surface-analysis feature (chat completions).
# In Azure this is set as an app setting of the same name, not from this file.
OPEN_AI_API=
```

- [ ] **Step 3: Install test deps into the preview venv**

Run:
```bash
uv pip install --python .preview-venv/bin/python pytest azure-storage-blob python-dotenv requests
```
Expected: installs succeed (plotly/numpy already present).

- [ ] **Step 4: Create `tests/conftest.py`**

```python
import importlib.util
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)  # so `import utils` works


def load_module(relpath, name):
    """Load a single module file directly, bypassing package __init__."""
    spec = importlib.util.spec_from_file_location(
        name, os.path.join(ROOT, relpath))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod
```

- [ ] **Step 5: Commit**

```bash
git add requirements.txt .env.example tests/conftest.py
git commit -m "Add AI-analysis scaffolding: dotenv dep, .env.example, test harness"
```

---

### Task 2: Blob cache helpers in `utils`

**Files:**
- Modify: `utils/__init__.py` (add three functions after `data_frame_from_blob`, around line 119)
- Test: `tests/test_utils_blobs.py`

- [ ] **Step 1: Write the failing tests**

`tests/test_utils_blobs.py`:
```python
from datetime import datetime
from unittest.mock import MagicMock

import utils


def test_get_analysis_blob_name():
    name = utils.get_analysis_blob_name("TSLA", datetime(2026, 5, 24))
    assert name == "TSLA_analysis_20260524.txt"


def test_write_then_read_text_blob_roundtrip():
    store = {}
    container = MagicMock()

    def get_blob_client(name):
        bc = MagicMock()
        bc.upload_blob.side_effect = lambda data, overwrite: store.__setitem__(name, data)
        bc.download_blob.return_value.readall.return_value = store.get(name, b"")
        return bc

    container.get_blob_client.side_effect = get_blob_client

    utils.write_text_blob(container, "x.txt", "héllo")
    assert utils.read_text_blob(container, "x.txt") == "héllo"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.preview-venv/bin/python -m pytest tests/test_utils_blobs.py -v`
Expected: FAIL — `AttributeError: module 'utils' has no attribute 'get_analysis_blob_name'`.

- [ ] **Step 3: Implement the helpers**

In `utils/__init__.py`, immediately after `data_frame_from_blob` (line ~119), add:
```python
def get_analysis_blob_name(ticker: str, date: datetime) -> str:
    return f"{ticker}_analysis_{date.strftime('%Y%m%d')}.txt"


def read_text_blob(container_client, blob_name: str) -> str:
    blob_data = container_client.get_blob_client(
        blob_name).download_blob().readall()
    return blob_data.decode("utf-8")


def write_text_blob(container_client, blob_name: str, text: str) -> None:
    container_client.get_blob_client(blob_name).upload_blob(
        text.encode("utf-8"), overwrite=True)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `.preview-venv/bin/python -m pytest tests/test_utils_blobs.py -v`
Expected: 2 passed.

- [ ] **Step 5: Commit**

```bash
git add utils/__init__.py tests/test_utils_blobs.py
git commit -m "Add analysis blob cache helpers to utils"
```

---

### Task 3: `coarse_grid` in `analysis.py`

**Files:**
- Create: `volatility_surface/analysis.py`
- Test: `tests/test_analysis.py`

- [ ] **Step 1: Write the failing test**

`tests/test_analysis.py`:
```python
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.preview-venv/bin/python -m pytest tests/test_analysis.py -v`
Expected: FAIL — `ModuleNotFoundError` / `AttributeError: coarse_grid`.

- [ ] **Step 3: Create `volatility_surface/analysis.py` with `coarse_grid`**

```python
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `.preview-venv/bin/python -m pytest tests/test_analysis.py -v`
Expected: 1 passed.

- [ ] **Step 5: Commit**

```bash
git add volatility_surface/analysis.py tests/test_analysis.py
git commit -m "Add coarse_grid surface downsampler"
```

---

### Task 4: `build_prompt` in `analysis.py`

**Files:**
- Modify: `volatility_surface/analysis.py`
- Test: `tests/test_analysis.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/test_analysis.py`:
```python
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.preview-venv/bin/python -m pytest tests/test_analysis.py::test_build_prompt_structure -v`
Expected: FAIL — `AttributeError: build_prompt`.

- [ ] **Step 3: Add `build_prompt` to `analysis.py`**

Append after `coarse_grid`:
```python
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `.preview-venv/bin/python -m pytest tests/test_analysis.py -v`
Expected: all passed.

- [ ] **Step 5: Commit**

```bash
git add volatility_surface/analysis.py tests/test_analysis.py
git commit -m "Add build_prompt for surface analysis"
```

---

### Task 5: `_call_openai` + `analyse_surface`

**Files:**
- Modify: `volatility_surface/analysis.py`
- Test: `tests/test_analysis.py`

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_analysis.py`:
```python
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.preview-venv/bin/python -m pytest tests/test_analysis.py -k call_openai -v`
Expected: FAIL — `AttributeError: _call_openai`.

- [ ] **Step 3: Add `_call_openai` and `analyse_surface`**

Append to `analysis.py`:
```python
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `.preview-venv/bin/python -m pytest tests/test_analysis.py -v`
Expected: all passed.

- [ ] **Step 5: Commit**

```bash
git add volatility_surface/analysis.py tests/test_analysis.py
git commit -m "Add OpenAI call + analyse_surface orchestrator"
```

---

### Task 6: `/api/surface-analysis` endpoint

**Files:**
- Modify: `volatility_surface/__init__.py` (add an `analysis` blueprint after the `renderer` blueprint)
- Modify: `function_app.py`

- [ ] **Step 1: Add the blueprint + route**

At the top of `volatility_surface/__init__.py`, add to the imports:
```python
import json
from volatility_surface.surface_page import render_surface_html
from volatility_surface.analysis import coarse_grid, analyse_surface
from volatility_surface.surface_page import compute_stats
```
(Keep the existing `render_surface_html` import; add the two new lines.)

At the end of the file, add:
```python
analysis = func.Blueprint()


@analysis.function_name(name="SurfaceAnalysisTrigger")
@analysis.route(route="surface-analysis", auth_level=func.AuthLevel.ANONYMOUS)
def get_surface_analysis(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('surface_analysis: start')

    ticker = req.params.get('ticker', 'SPY').strip().upper()
    date_str = req.params.get(
        'date', datetime.strftime(datetime.now(), '%Y-%m-%d'))
    try:
        date = datetime.strptime(date_str, '%Y-%m-%d')
    except ValueError:
        return func.HttpResponse("Invalid date format. Please use YYYY-MM-DD.",
                                 status_code=400)

    container = utils.setup_blob_container()
    blob_name = utils.get_analysis_blob_name(ticker, date)

    # Cache hit -> return immediately, no model call.
    if container is not None and utils.blob_exists(container, blob_name):
        cached = utils.read_text_blob(container, blob_name)
        return func.HttpResponse(
            json.dumps({"analysis": cached, "cached": True}),
            mimetype="application/json")

    # Cache miss -> build the surface, derive stats + grid, ask the model.
    try:
        df = get_option_data(ticker, date)
        spot = float(df['spot'].iloc[0]) if 'spot' in df.columns and not df.empty else None
        strikes, tenors, vol_surface = build_surface(df, ticker)
        if spot is None:
            spot = float(np.mean(strikes)) if len(strikes) else 0.0
        stats = compute_stats(strikes, tenors, vol_surface, spot)
        grid = coarse_grid(strikes, tenors, vol_surface)
        text = analyse_surface(ticker, date_str, stats, grid)
    except Exception:
        logging.exception("surface_analysis: failed to build analysis")
        text = None

    if text and container is not None:
        try:
            utils.write_text_blob(container, blob_name, text)
        except Exception:
            logging.exception("surface_analysis: failed to cache analysis")

    return func.HttpResponse(
        json.dumps({"analysis": text, "cached": False}),
        mimetype="application/json")
```

Note: this needs `numpy`. Add `import numpy as np` to the imports at the top of `volatility_surface/__init__.py` (it was removed in an earlier change; re-add it).

- [ ] **Step 2: Register the blueprint in `function_app.py`**

Modify `function_app.py` to:
```python
import azure.functions as func
from volatility_surface import option_data, renderer, analysis
from downloader_trigger import downloader_trigger

app = func.FunctionApp()

app.register_functions(renderer)
app.register_functions(option_data)
app.register_functions(analysis)
app.register_functions(downloader_trigger)
```

- [ ] **Step 3: Compile-check (no azure runtime locally)**

Run:
```bash
.preview-venv/bin/python -c "import ast; ast.parse(open('volatility_surface/__init__.py').read()); ast.parse(open('function_app.py').read()); print('parse OK')"
```
Expected: `parse OK`.

- [ ] **Step 4: Commit**

```bash
git add volatility_surface/__init__.py function_app.py
git commit -m "Add /api/surface-analysis endpoint with blob caching"
```

---

### Task 7: UI block + fetch JS in `surface_page.py`

**Files:**
- Modify: `volatility_surface/surface_page.py`

- [ ] **Step 1: Add CSS for the analysis block**

In the `<style>` block, immediately after the `.panel-bar` rules (search for `.panel-bar .hint svg`), add:
```css
.ai-note{display:flex;gap:13px;align-items:flex-start;padding:15px 20px;
  border-bottom:1px solid var(--line);background:rgba(141,155,232,.04)}
.ai-note.hide{display:none}
.ai-badge{flex:none;display:inline-flex;align-items:center;gap:6px;margin-top:1px;
  font-family:var(--mono);font-size:9px;letter-spacing:.18em;text-transform:uppercase;
  color:var(--peri)}
.ai-badge svg{width:12px;height:12px}
.ai-text{font-size:13px;line-height:1.55;color:var(--muted)}
.ai-text b{color:var(--text);font-weight:600}
.ai-note.loading .ai-text{color:var(--dim);font-style:italic}
@keyframes aipulse{0%,100%{opacity:.4}50%{opacity:.9}}
.ai-note.loading .ai-text{animation:aipulse 1.4s ease-in-out infinite}
```

- [ ] **Step 2: Add the markup directly below `.panel-bar`**

Find the `.panel-bar` `</div>` that closes the chart header (the block containing `Drag to rotate`). Immediately after it, before `<div class="plot-wrap">`, insert:
```html
      <div class="ai-note loading hide" id="ai-note">
        <span class="ai-badge">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
            <path d="M12 3l1.8 4.7L18 9l-4.2 1.3L12 15l-1.8-4.7L6 9l4.2-1.3z" stroke-linejoin="round"/>
          </svg>AI generated
        </span>
        <span class="ai-text" id="ai-text">Analyzing surface</span>
      </div>
```

- [ ] **Step 3: Add the fetch JS**

In the bottom `<script>`, after the Plotly lazy-load IIFE (before `</script>`), add:
```javascript
// Fetch the AI analysis after load; reveal the note or leave it hidden.
(function(){
  var note = document.getElementById('ai-note'),
      out = document.getElementById('ai-text');
  if(!note || !out) return;
  var params = new URLSearchParams(window.location.search);
  var ticker = (params.get('ticker') || '$ticker'),
      date = (params.get('date') || '$date');
  note.classList.remove('hide');               // show loading state
  fetch('/api/surface-analysis?ticker=' + encodeURIComponent(ticker) +
        '&date=' + encodeURIComponent(date))
    .then(function(r){ return r.ok ? r.json() : {analysis:null}; })
    .then(function(d){
      if(d && d.analysis){ out.textContent = d.analysis; note.classList.remove('loading'); }
      else { note.classList.add('hide'); }
    })
    .catch(function(){ note.classList.add('hide'); });
})();
```

Note: `$ticker` and `$date` are substituted server-side as fallbacks; the live query string is the primary source. These are already passed to `safe_substitute` in `render_page`, so no Python change is needed.

- [ ] **Step 4: Verify it compiles and the markers render**

Run:
```bash
.preview-venv/bin/python -m py_compile volatility_surface/surface_page.py && \
.preview-venv/bin/python scripts/preview_surface.py --ticker TSLA --out preview.html && \
grep -c "ai-note\|ai-text\|surface-analysis" preview.html
```
Expected: compiles; grep count >= 3.

- [ ] **Step 5: Commit**

```bash
git add volatility_surface/surface_page.py
git commit -m "Add AI-analysis note UI below the chart header"
```

---

### Task 8: Local preview stub for the analysis endpoint

**Files:**
- Modify: `scripts/serve_preview.py`

- [ ] **Step 1: Add a stub route so the UI is previewable without OpenAI**

In `scripts/serve_preview.py`, inside `Handler.do_GET`, before the existing surface handling, add a branch:
```python
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
```
Place it right after the `parsed = urlparse(self.path)` line and the 404 guard — i.e., handle the analysis path before the page-rendering branch. Adjust the existing 404 path check to also allow `/api/surface-analysis`.

- [ ] **Step 2: Restart the preview server**

Run (kill by port to avoid matching this shell):
```bash
fuser -k 8050/tcp 2>/dev/null; .preview-venv/bin/python scripts/serve_preview.py --port 8050 &
sleep 1
curl -s "http://127.0.0.1:8050/api/surface-analysis?ticker=NVDA"
```
Expected: JSON with a non-null `analysis` string.

- [ ] **Step 3: Screenshot to confirm the note renders**

Run:
```bash
google-chrome-stable --headless=new --disable-gpu --no-sandbox --enable-unsafe-swiftshader --window-size=1500,1200 --virtual-time-budget=13000 --screenshot=/tmp/ai.png "http://127.0.0.1:8050/?ticker=NVDA" >/dev/null 2>&1; echo "size=$(stat -c%s /tmp/ai.png)"
```
Then view `/tmp/ai.png` and confirm the "AI generated" note sits below the chart header with the stub text.

- [ ] **Step 4: Commit**

```bash
git add scripts/serve_preview.py
git commit -m "Serve a stub analysis in the local preview server"
```

---

### Task 9: Documentation

**Files:**
- Modify: `README.md`, `CLAUDE.md`

- [ ] **Step 1: Document the endpoint + env var in `README.md`**

In the Endpoints table, add a row:
```
| `/api/surface-analysis` | AI explanation of the surface (JSON) | `?ticker=TSLA` |
```
In the Deployment section's app-settings note, add: "`OPEN_AI_API` — OpenAI key for the AI analysis feature; set as an app setting (never committed)."

- [ ] **Step 2: Document in `CLAUDE.md`**

Add under the endpoints list: "`/api/surface-analysis?ticker=&date=` → JSON `{analysis, cached}`; gpt-4o-mini, blob-cached per ticker+date; key from `OPEN_AI_API`."

- [ ] **Step 3: Commit**

```bash
git add README.md CLAUDE.md
git commit -m "Document the AI surface-analysis endpoint and OPEN_AI_API"
```

---

### Task 10: Deploy + set the Azure secret + verify live

**Files:** none (infra)

- [ ] **Step 1: Set `OPEN_AI_API` as an app setting (value from local .env, never printed)**

Run:
```bash
KEY=$(grep -E '^OPEN_AI_API=' .env | cut -d= -f2-)
az functionapp config appsettings set -n volsurface -g volsurface \
  --settings "OPEN_AI_API=$KEY" >/dev/null && echo "app setting set"
unset KEY
```
Expected: `app setting set`. (If this command is permission-gated, ask the user to run it or set it in the Portal.)

- [ ] **Step 2: Push to trigger CI/CD deploy**

```bash
git push origin main
```
Then watch the latest `deploy.yml` run to success with `gh run watch`.

- [ ] **Step 3: Verify the live endpoint**

Run:
```bash
curl -s --max-time 90 "https://volsurface.azurewebsites.net/api/surface-analysis?ticker=SPY"
```
Expected: JSON `{"analysis": "<non-null text>", "cached": false}` on first call; a second call returns `"cached": true`.

- [ ] **Step 4: Verify the page shows the note**

Open `https://volsurface.azurewebsites.net/api/volatility-surface?ticker=SPY` and confirm the "AI generated" note appears below the chart header once the fetch resolves.

---

## Self-Review Notes

- **Spec coverage:** config/secret (Task 1, 10), endpoint + cache (Task 6, utils Task 2), prompt + model + grid (Tasks 3-5), UI below panel-bar with badge + graceful hide (Task 7), docs + .env.example (Tasks 1, 9), deploy app setting (Task 10). All covered.
- **No-dash requirement:** enforced in the system prompt and stripped in `_call_openai` (`_DASHES`), tested in Task 5.
- **Type consistency:** `analyse_surface(ticker, date_str, stats, grid)` and `coarse_grid(strikes, tenors, vol_surface, n=5)` signatures match their call sites in Task 6; `compute_stats` keys (`spot/atm_iv/skew/term/iv_lo/iv_hi/tenor_span/n_strikes`) match `build_prompt`.
- **Failure path:** endpoint returns `{"analysis": null}` (never raises); UI hides on null/error.
