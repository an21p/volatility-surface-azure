# Volatility Surface Functions

An Azure Functions backend that fetches, filters, and serves options data from
the CBOE JSON endpoint, and renders an interactive implied-volatility surface.

## Features

- **Scheduled data download** — a timer trigger periodically fetches and stores
  options data for selected tickers from the CBOE API.
- **Data filtering** — extracts an out-of-the-money option blend across a wide
  moneyness band for every upcoming standard (third-Friday) expiry.
- **HTTP API** — returns filtered options data (JSON) for a ticker and date.
- **Rendered surface page** — an HTTP endpoint returns a styled, self-contained
  HTML page: a calibrated SSVI surface drawn with Plotly, wrapped in a
  dark "instrument readout" (derived stats, ticker/date form). The 3D renderer
  is **lazy-loaded** — the page and stats paint immediately while Plotly.js is
  fetched on idle and drawn into a skeleton placeholder.
- **Azure Blob Storage** — stores raw and filtered options data.
- **Desktop visualiser** — a matplotlib script to plot the surface locally.

## Endpoints

Base URL (production): `https://volsurface.azurewebsites.net`

| Route | Returns | Example |
| --- | --- | --- |
| `/api/volatility-surface` | Styled HTML surface page | `?ticker=TSLA&date=2026-05-24` |
| `/api/option-data` | Filtered options data (JSON) | `?ticker=SPY` |
| `/api/surface-analysis` | AI explanation of the surface (JSON) | `?ticker=TSLA` |

Both accept `ticker` (default `SPY`) and `date` (`YYYY-MM-DD`, default today).

## Understanding the surface

An **implied volatility surface** is the market's view of risk for one
underlying, read off option prices. Every option price implies a single
volatility number — the value that makes a Black–Scholes model reproduce the
quoted price. Plot that number across all strikes and expiries and you get a
surface, not a flat sheet, which is exactly the interesting part.

The page plots three axes:

- **Strike (x)** — the option's exercise price. Lower strikes (left) are
  downside puts; higher strikes (right) are upside calls.
- **Tenor (y)** — time to expiry, in years.
- **Implied volatility (z)** — annualised, in %. Encoded by both height and
  colour: cool indigo = calm (low IV), warm amber = stressed (high IV).

Two things to read off the shape:

- **Skew / smile (across strike).** A textbook-flat market would be level; in
  practice IV rises toward low strikes (downside puts are bid up for crash
  protection) and often curls up at both wings (the "smile"). The **Skew ·
  90/110** stat quantifies it as IV(90% strike) − IV(110% strike) in vol points
  — positive is the classic equity put skew.
- **Term structure (across tenor).** How IV changes with time to expiry. The
  **Term · F→B** stat is back-month minus front-month ATM IV: positive
  (contango) is the calm default; negative (backwardation) flags near-term
  stress.

### How it's built

1. Take an **out-of-the-money blend** across a wide moneyness band (~70–130% of
   spot) for every upcoming standard (third-Friday) expiry — puts below spot,
   calls above — since OTM quotes have the tightest spreads and cleanest IV.
2. For each option, solve the **implied volatility** from the bid/ask **mid**
   price with a Black–Scholes–Merton model (QuantLib), assuming flat `r = 3%`
   and `q = 1%`.
3. Calibrate an **SSVI** (surface SVI) model to the resulting points: a per-expiry
   ATM total variance plus three global parameters. SSVI gives a genuinely smooth
   smile and is calendar-arbitrage-free by construction (with a butterfly-arbitrage
   safeguard). If calibration fails it falls back to a QuantLib
   `BlackVarianceSurface`.
4. Evaluate the model on a fixed moneyness × tenor grid to draw.

The readout panel summarises the surface: **Spot**, **ATM IV** (at-the-money,
~1-month), **Skew**, **Term**, the **IV range** (min–max across the grid), the
**tenor span**, and the number of **strikes**.

> Figures are model-derived from delayed CBOE data with flat rate/dividend
> assumptions — illustrative, not trading-grade marks.

## Structure

- `function_app.py` — registers all Azure Functions.
- `volatility_surface/__init__.py` — HTTP triggers: `option-data` (JSON) and
  `volatility-surface` (HTML).
- `volatility_surface/surface_page.py` — presentation layer: figure styling,
  derived stats, and the HTML page template.
- `downloader_trigger/` — timer-triggered scheduled data download.
- `visualiser/` — builds the QuantLib surface; also a matplotlib CLI.
- `utils/` — data fetching, filtering, and blob storage helpers.
- `scripts/` — local frontend preview tooling (synthetic data, no Azure needed);
  **not deployed** (see `.funcignore`).

## Requirements

- Python 3.12
- [Azure Functions Core Tools v4](https://learn.microsoft.com/azure/azure-functions/functions-run-local) (`func`)
- Azure CLI (`az`) — for deployment
- An Azure Storage account

See [requirements.txt](requirements.txt) for Python dependencies.

## Local development

Configure storage, then start the runtime:

```bash
cp example.local.settings.json local.settings.json   # add your connection string
func start                                            # serves on http://localhost:7071
```

- Surface page: <http://localhost:7071/api/volatility-surface?ticker=TSLA>
- Desktop plot: `python visualiser/__init__.py --ticker TSLA`

### Preview the frontend without Azure

The page design can be iterated on with synthetic data — no storage, CBOE, or
QuantLib required:

```bash
uv venv .preview-venv && uv pip install --python .preview-venv/bin/python plotly==6.1.2 numpy

# one-off static file:
.preview-venv/bin/python scripts/preview_surface.py --ticker TSLA --out preview.html

# live server with a ticker/date form (synthetic, per-ticker surfaces):
.preview-venv/bin/python scripts/serve_preview.py --port 8050   # http://localhost:8050
```

To preview the **real** SSVI pipeline (not synthetic) you need QuantLib + scipy,
so use a full env and point `--data` at an option chain:

```bash
python scripts/make_sample_chain.py                              # regenerate fixture
python scripts/serve_preview.py --data tests/fixtures/options_sample.csv --port 8050
```

## Deployment

The production app is an **Azure Functions Flex Consumption** app:

| | |
| --- | --- |
| Function app | `volsurface` |
| Resource group | `volsurface` |
| Subscription | `AzureCyan` |
| Plan | Flex Consumption (Linux, Python 3.12) |
| URL | `https://volsurface.azurewebsites.net` |

### Automatic (CI/CD)

Pushing to `main` triggers [`.github/workflows/deploy.yml`](.github/workflows/deploy.yml),
which deploys to the function app via `azure/login` + `Azure/functions-action`
(Flex Consumption, remote build). Docs-only changes (`**.md`) are skipped. Auth
uses a service principal stored in the `AZURE_CREDENTIALS` repository secret,
scoped to **only** the `volsurface` function app.

To (re)create that service principal and secret:

```bash
az ad sp create-for-rbac --name "gh-volsurface-deploy" --role contributor \
  --scopes "/subscriptions/<SUB_ID>/resourceGroups/volsurface/providers/Microsoft.Web/sites/volsurface" \
  --sdk-auth | gh secret set AZURE_CREDENTIALS --repo an21p/volatility-surface-azure
```

### Manual (fallback)

Flex Consumption builds dependencies remotely, so deploy with Core Tools and
`--build remote`:

```bash
az login                                       # account needs Contributor on the app
func azure functionapp publish volsurface --build remote --python
```

App settings (storage connection strings, App Insights, and `OPEN_AI_API` for
the AI analysis feature) are configured on the app and are **not** stored in
this repo. Locally, `OPEN_AI_API` is read from `.env` (see `.env.example`).

### Notes / gotchas

- **Flex Consumption requires RBAC for tooling.** It does **not** support the
  classic `az functionapp deployment source config-zip`, nor a publish-profile
  based GitHub Action — `Azure/functions-action` on Flex needs an `azure/login`
  (service principal / OIDC) step, which is why CI/CD uses `AZURE_CREDENTIALS`.
- `func ... publish` needs `--python` when there is no `local.settings.json`.
- The `scripts/` preview tooling and `.preview-venv/` are excluded from the
  deployment package via `.funcignore` and `.gitignore`.
