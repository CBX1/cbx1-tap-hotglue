# Agent Guide — tap-cbx1

**`README.md` is the authoritative documentation** for this repo: layout, config, env vars, API surface, pagination/state design, tests, and the debugging playbook. Read it first; don't duplicate its content here.

Agent-specific ground rules:

- **Do not change the pagination or state model** (keyset cursor, pinned windows, manual watermark) without reading README → "Pagination & state" *and* the docstrings in `tap_cbx1/client.py`. Several "obvious simplifications" (skip paging, SDK auto-watermark, `$ne updatedBy` push-down) are deliberately rejected with prod evidence.
- Never commit `config.json`, `.env`, `catalog.json`, `output.singer`, or `state.json` — a run writes the JWT session token back into `config.json`. All are gitignored.
- Run `poetry run pytest` (35 tests, <1s, no network) before and after touching `client.py` or `streams.py` — the suite encodes the pagination/state contract.
- Local run/debug workflow: README → "Running locally" and "Debugging playbook"; Singer state semantics: `docs/singer-format.md`.
- End-to-end pipeline context: `docs/architecture.md` in `hotglue-transformation-scripts`.
