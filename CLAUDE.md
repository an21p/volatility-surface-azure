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

## Deployment

Production app: **`volsurface`** / RG `volsurface` / subscription `AzureCyan` /
**Flex Consumption**, Linux, Python 3.12 → <https://volsurface.azurewebsites.net>

**Automatic:** pushing to `main` runs `.github/workflows/deploy.yml`, which
deploys via `azure/login` + `Azure/functions-action` (remote build). Auth is a
service principal in the `AZURE_CREDENTIALS` secret, scoped to the function app.
Docs-only (`**.md`) pushes are skipped.

**Manual** (fallback / pre-push build check):

```bash
az login                                       # needs Contributor on the app
func azure functionapp publish volsurface --build remote --python
```

### Important constraints

- **Flex Consumption.** Remote build is required (`--build remote`). The classic
  `az functionapp deployment source config-zip` is **not** supported, and a
  publish-profile GitHub Action does **not** work — `Azure/functions-action`
  needs an `azure/login` (service principal / OIDC) step.
- `func ... publish` needs `--python` when there's no `local.settings.json`.
- Re-create the deploy SP: `az ad sp create-for-rbac --name gh-volsurface-deploy
  --role contributor --scopes <function-app-resource-id> --sdk-auth | gh secret
  set AZURE_CREDENTIALS --repo an21p/volatility-surface-azure`.
- Adding a dependency means editing `requirements.txt`; Flex installs it during
  the remote build on the next deploy.
