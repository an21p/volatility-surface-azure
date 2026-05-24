# AI-generated volatility-surface analysis

**Date:** 2026-05-24
**Status:** Approved (design)

## Goal

Show a short, plain-English, AI-generated explanation of the rendered implied
volatility surface, directly below the chart header (`.panel-bar`), clearly
labelled as AI generated. The text is produced by the OpenAI chat completions
API from the surface's summary statistics and a coarse IV grid.

## Decisions

- **Architecture:** lazy + cached. The page renders instantly; the analysis is
  fetched client-side after load from a dedicated endpoint. Results are cached
  in blob storage per ticker+date, so repeat views cost nothing and are instant.
- **Model:** `gpt-4o-mini` (chat completions), `max_tokens ≈ 160`,
  `temperature 0.4`.
- **Data sent:** derived stats + a coarse ~5×5 downsampled IV grid.
- **Secret:** `OPEN_AI_API` read from `os.environ`. Production reads it from an
  Azure **app setting**; local dev loads it from `.env` via `python-dotenv`.
  `.env` is gitignored and in `.funcignore` (never deployed).

## Components

### 1. `volatility_surface/analysis.py` (new)
Pure analysis logic, no Azure Functions types.

- `coarse_grid(strikes, tenors, vol_surface, n=5) -> dict` — downsample to ~5×5,
  return `{"strikes": [...], "tenors": [...], "iv": [[...]]}` (IV in %).
- `build_prompt(ticker, date_str, stats, grid) -> list[dict]` — system + user
  chat messages. System: act as a quantitative analyst; 2–3 sentences (~60
  words); plain English; specific with numbers; no financial advice; **no
  em-dashes**. User: JSON payload of stats + grid.
- `generate_analysis(ticker, date_str, stats, grid) -> Optional[str]` — POST to
  `https://api.openai.com/v1/chat/completions` via `requests`, bearer
  `OPEN_AI_API`, ~15s timeout. Returns the stripped text, or `None` on any
  failure (missing key, non-200, timeout, malformed response). Strips any
  em/en-dashes from the result as a safety net.

### 2. `utils/__init__.py` (additions)
- `get_analysis_blob_name(ticker, date) -> str` → `f"{ticker}_analysis_{date:%Y%m%d}.txt"`.
- `read_text_blob(container_client, name) -> Optional[str]`.
- `write_text_blob(container_client, name, text) -> None` (overwrite=True).

### 3. `volatility_surface/__init__.py` (new `analysis` blueprint)
`@analysis.route(route="surface-analysis")`, anonymous. Flow:

1. Parse `ticker` / `date` (same validation as existing routes).
2. `container = utils.setup_blob_container()`; `name = get_analysis_blob_name(...)`.
3. **Cache hit** (`blob_exists`) → return `{"analysis": <cached>, "cached": true}`.
4. **Miss** → `df = get_option_data(ticker, date)`;
   `spot = float(df['spot'].iloc[0])` (as in the render route);
   `strikes, tenors, vol = build_surface(df, ticker)`;
   `stats = compute_stats(strikes, tenors, vol, spot)`;
   `grid = coarse_grid(...)`; `text = generate_analysis(...)`.
   - If `text` is not None → `write_text_blob(...)` then return it.
   - If `text` is None → return `{"analysis": null}` (do not cache).
5. Response JSON: `{"analysis": <str|null>, "cached": <bool>}`,
   `mimetype="application/json"`. Failures never raise to the client.

### 4. `function_app.py`
Register the new `analysis` blueprint alongside the others.

### 5. `volatility_surface/surface_page.py` (UI + JS)
- New block inside `.panel`, **directly below `.panel-bar`**, above `.plot-wrap`:
  an `AI GENERATED` badge (mono, small sparkle icon, subtle accent) plus the
  analysis paragraph. Styled to match the dark theme.
- States: loading shimmer ("Analyzing surface") by default; filled when the
  fetch resolves; the whole block is `display:none` if `analysis` is null.
- JS: on load, `fetch('/api/surface-analysis?ticker=&date=')`, populate or hide.
  Independent of the Plotly lazy-load.

### 6. `requirements.txt`
Add `python-dotenv`. Call `load_dotenv()` once at import in `analysis.py`.

### 7. Docs + scaffolding
- `.env.example` (new): `OPEN_AI_API=` placeholder.
- README / CLAUDE: document the env var, the Azure app setting, and the new
  `/api/surface-analysis` endpoint.

## Data flow

```
page load ──► GET /api/volatility-surface  ──► HTML (surface lazy-loads)
          └─► GET /api/surface-analysis ──► [blob cache hit?] ─yes─► cached text
                                                  │no
                                                  ▼
                                     build surface ► stats ► coarse grid
                                                  ▼
                                     OpenAI gpt-4o-mini ► text ► cache ► return
```

## Error handling

- Missing `OPEN_AI_API`, non-200, timeout, or bad payload → `generate_analysis`
  returns `None` → endpoint returns `{"analysis": null}` → UI hides the block.
  The surface page is never affected.
- Failures are logged but not cached, so a later request can retry.

## Cost & caching

- One OpenAI call per unique ticker+date; subsequent views served from blob.
- `gpt-4o-mini` + ~160 max tokens keeps per-call cost negligible.
- Cache key is ticker+date; acceptable staleness (the underlying filtered data
  for a given date is effectively fixed once stored).

## Out of scope

- Regenerating/refreshing analysis on demand.
- Streaming the response.
- Analysis history or multiple variants per ticker+date.

## Deployment note

`OPEN_AI_API` must exist as an Azure app setting on `volsurface` for the live
site. Set via `az functionapp config appsettings set` (value read from local
`.env`, never printed) or in the Portal.
