# CLAUDE.md — volatility-surface-azure

Azure Functions (Python 3.12) backend that serves CBOE options data and renders
an implied-volatility surface page. See `README.md` for full details.

## Layout

- `function_app.py` — registers the blueprints.
- `volatility_surface/__init__.py` — HTTP triggers: `option-data` (JSON),
  `volatility-surface` (HTML). Builds the surface via `visualiser.build_surface`.
- `volatility_surface/surface_page.py` — **all** page presentation: Plotly
  figure styling, derived stats, and the HTML template. The graph is embedded
  as JSON and Plotly.js is lazy-loaded client-side (page/stats paint first).
- `downloader_trigger/` — timer trigger. `utils/` — fetch/filter/blob helpers.
- `scripts/` — local preview tooling only; **not deployed** (`.funcignore`).

## Endpoints

- `/api/volatility-surface?ticker=TSLA&date=YYYY-MM-DD` → styled HTML surface.
- `/api/option-data?ticker=SPY` → filtered options JSON.

## Local dev

```bash
cp example.local.settings.json local.settings.json   # needs a storage conn string
func start                                            # http://localhost:7071
```

Frontend-only iteration (synthetic data, no Azure/CBOE/QuantLib):

```bash
uv venv .preview-venv && uv pip install --python .preview-venv/bin/python plotly==6.1.2 numpy
.preview-venv/bin/python scripts/serve_preview.py --port 8050   # http://localhost:8050
```

## Deployment (manual)

Production app: **`volsurface`** / RG `volsurface` / subscription `AzureCyan` /
**Flex Consumption**, Linux, Python 3.12 → <https://volsurface.azurewebsites.net>

```bash
az login                                       # needs Contributor on the app
func azure functionapp publish volsurface --build remote
```

### Important constraints

- **Flex Consumption.** Remote build is required (`--build remote`). The classic
  `az functionapp deployment source config-zip` is **not** supported.
- **No CI/CD.** A publish-profile GitHub Action does **not** work on Flex —
  `Azure/functions-action` needs an `azure/login` (service principal / OIDC)
  step. Auto-deploy was intentionally skipped: creating that identity needs IAM
  rights the available (guest) account lacks. If revisiting CI/CD, set up an SP
  or OIDC federated credential and add an `azure/login` step before the action.
- Adding a dependency means editing `requirements.txt`; Flex installs it during
  the remote build on the next deploy.
