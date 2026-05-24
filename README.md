# Volatility Surface Functions

An Azure Functions backend that fetches, filters, and serves options data from
the CBOE JSON endpoint, and renders an interactive implied-volatility surface.

## Features

- **Scheduled data download** — a timer trigger periodically fetches and stores
  options data for selected tickers from the CBOE API.
- **Data filtering** — extracts near-the-money call options for the next
  standard (third-Friday) expiry.
- **HTTP API** — returns filtered options data (JSON) for a ticker and date.
- **Rendered surface page** — an HTTP endpoint returns a styled, self-contained
  HTML page: a QuantLib Black-variance surface drawn with Plotly, wrapped in a
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

Both accept `ticker` (default `SPY`) and `date` (`YYYY-MM-DD`, default today).

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

## Deployment

The production app is an **Azure Functions Flex Consumption** app:

| | |
| --- | --- |
| Function app | `volsurface` |
| Resource group | `volsurface` |
| Subscription | `AzureCyan` |
| Plan | Flex Consumption (Linux, Python 3.12) |
| URL | `https://volsurface.azurewebsites.net` |

Deploys are **manual** (no CI/CD). Flex Consumption builds dependencies
remotely, so deploy with Core Tools and `--build remote`:

```bash
az login                                       # account needs Contributor on the app
func azure functionapp publish volsurface --build remote
```

App settings (storage connection strings, App Insights) are configured on the
app and are **not** stored in this repo.

### Notes / gotchas

- **Flex Consumption is required-RBAC for tooling.** It does **not** support the
  classic `az functionapp deployment source config-zip`, nor a publish-profile
  based GitHub Action — `Azure/functions-action` on Flex needs an `azure/login`
  (service principal / OIDC) step. CI/CD was intentionally skipped because the
  available account lacks rights to create the required identity.
- The `scripts/` preview tooling and `.preview-venv/` are excluded from the
  deployment package via `.funcignore` and `.gitignore`.
