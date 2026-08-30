# AGENTS.md — Flux backend

Scope: the repository root and everything except `mobile/`, which has its own
`mobile/AGENTS.md`.

The binding architectural rules live in [`.qoder/rules/Flux-sys.md`](.qoder/rules/Flux-sys.md).
This file is the operational companion: what to run, and the traps in this codebase.

## Commands

```bash
# setup
python -m venv .venv
.venv/bin/pip install -r requirements.txt -r requirements-dev.txt

# gates — both must pass before a change is done
.venv/bin/pytest -q
.venv/bin/ruff check app tests

# run
./start.sh
```

Never run `pip install` for a package that is not in `requirements.txt` or
`requirements-dev.txt`. If a new dependency is genuinely needed, add it there with
a pin, in the same change.

## Hard constraints

- **No local inference.** No `torch`, no `ultralytics`, no `*.pt` weights, no local
  GPU fallback. Inference is an HTTP call to Roboflow (or another external API).
  `.gitignore` and CI both reject committed weights.
- **No hardcoded credentials.** Everything through `app/config.py` /
  environment variables. New settings must be added to `.env.example` too.
- **Type-hint every function signature.**
- Degrade, don't crash: a missing media file or an upstream API timeout returns a
  typed error or an empty result.
- **Never fabricate model input.** No blank/placeholder frames when a download or
  decode fails. Raise `MediaUnavailable` (transient, keeps the retry budget) or a
  `PermanentProcessingError` subclass (straight to the DLQ). A synthetic frame is
  billed by the API and then persisted as a successful zero-detection reading.

## Traps in this codebase

- `app/main.py` starts with `from __future__ import annotations`, which stringifies
  annotations. A missing import used **only** in a signature will not raise at
  import time — it will fail at request time. `ruff check` catches this as `F821`;
  run it, don't rely on the app starting.
- Development fallbacks (`fakeredis`, SQLite) are gated on
  `Settings.is_production`. Do not add a new fallback that is unconditional.
- The nginx route regex in `infra/nginx.conf` must be kept in sync with the route
  prefixes in `app/main.py`. A new top-level prefix that is not listed there is
  served as a static file and 404s in production.
- `tests/conftest.py` installs `sys.modules` stubs for `cv2` and `inference_sdk`.
  Tests that mock `app.main.get_settings` with a `MagicMock` must also set
  `is_production = False` and `missing_production_settings.return_value = []`,
  otherwise the startup guard trips.
- `app/database.py` is dead: nothing imports it, and it declares a second
  `declarative_base()` plus duplicate models. Do not add to it.
- Async tests use `@pytest.mark.anyio` plus a local `anyio_backend` fixture.
  `anyio` already ships with FastAPI, so there is no `pytest-asyncio` dependency;
  don't add one.
- `tests/conftest.py` force-assigns `ROBOFLOW_API_KEY` / `ROBOFLOW_MODEL_ID`. This
  is deliberate: it keeps the suite green without a `.env` and stops a test from
  ever billing the real account. Don't switch it to `setdefault`.

## State

Keep `/STATE.yaml` current: one update per completed milestone, not per edit.
