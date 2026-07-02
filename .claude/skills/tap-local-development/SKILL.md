---
name: tap-local-development
description: Run, test, and debug tap-cbx1 locally — discover/sync workflow, config and env setup, reading Singer output, incremental state runs, and the pagination/state debugging playbook. Use when running the tap, investigating missing/duplicated records, auth failures, or schema discovery issues.
---

# tap-cbx1 Local Development

Authoritative reference: `AGENTS.md` at the repo root (config keys, env vars, pagination/state design, debugging playbook). This skill is the operational workflow.

## Setup (once)

```bash
poetry install
cp .env.example .env    # fill in BASE_URL (+ optional HOTGLUE_PRINCIPAL_ID)
```

Create `config.json` with the real keys — `Code`, `OrgId`, `CRMSystem`, optional `page_size`/`start_date` (exact shape in `AGENTS.md` → Config). Get `Code`/`OrgId` from the tenant's Descope access key setup (ask in #eng-crm-self-serve if unsure). Never commit `config.json` — a run writes the JWT session token back into it (it is gitignored).

## Run

```bash
set -a; source .env; set +a           # bash/zsh; fish: use `export (cat .env)` equivalents
poetry run tap-cbx1 --config config.json --discover > catalog.json
poetry run tap-cbx1 --config config.json --catalog catalog.json > output.singer
```

- Logs go to **stderr**, Singer messages to **stdout** — always redirect stdout to a file.
- Incremental: add `--state state.json`. To capture next-run state, take the **last** STATE line from the previous output: `grep '"type": "STATE"' output.singer | tail -1 | python3 -c 'import sys,json; print(json.dumps(json.load(sys.stdin)["value"]))' > state.json`
- Stream selection: edit `catalog.json` metadata (`"selected": false`) to skip a stream.

## Reading the output (Singer format)

See `singer-format.md` in this skill directory for the SCHEMA/RECORD/STATE message primer and how this tap's state (`replication_key_value`, `cursor`, `window_end`) should evolve across a healthy run.

Quick checks:

```bash
grep -c '"type": "RECORD"' output.singer                      # record count
grep '"type": "STATE"' output.singer | tail -1                # final state
python3 -c "import json,sys; [print(json.loads(l)['record']['id']) for l in open('output.singer') if json.loads(l).get('type')=='RECORD']" | sort | uniq -d   # dup PKs
```

## Test

```bash
poetry run pytest
```

## Debug

Work the symptom table in `AGENTS.md` → "Debugging playbook" first. Key invariants to check when records are missing or duplicated:

1. Final STATE must have `replication_key_value` = the window upper bound, and **no** `cursor`/`window_end` keys (clean completion).
2. A mid-run STATE with `cursor` + `window_end` is a resume point — normal during a run, a wedge if it persists across runs.
3. Payloads must send `cursor`, never `pageNumber` (verify with the tests, or log the payload from `prepare_request_payload`).

## End-to-end check with the target

```bash
cat output.singer | (cd ../cbx1-target-hotglue && poetry run target-cbx1 --config config.json)
```

Requires a QA-tenant config in the target repo — see that repo's `AGENTS.md`.
