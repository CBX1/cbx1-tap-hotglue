# tap-cbx1

Singer **tap** (source connector) that extracts CRM data (accounts, contacts) from the CBX1 platform's Java backend API. Built on the [Meltano Singer SDK](https://sdk.meltano.com/). It is the read side of the HotGlue CBX1 ↔ CRM sync:

```
tap-cbx1  →  hotglue-transformation-scripts (etl.py)  →  CRM target (Salesforce/HubSpot/…)
CRM tap   →  hotglue-transformation-scripts (etl.py)  →  cbx1-target-hotglue
```

End-to-end pipeline documentation lives in the `hotglue-transformation-scripts` repo (`docs/architecture.md`).

## Quickstart

Requires **Python ≥3.7.1, <3.11** (`poetry install` fails on 3.11+; the pinned `singer-sdk 0.4.x` needs the older interpreter).

```bash
poetry install
cp .env.example .env        # fill in BASE_URL (+ optional HOTGLUE_PRINCIPAL_ID)
```

Create a `config.json` (see [Config](#config-configjson)), then:

```bash
poetry run tap-cbx1 --config config.json --discover > catalog.json
poetry run tap-cbx1 --config config.json --catalog catalog.json > output.singer
```

## Layout

| Path | Role |
|---|---|
| `tap_cbx1/tap.py` | `TapCBX1` — tap entry point, config schema, stream registry |
| `tap_cbx1/streams.py` | `ContactStream`, `AccountStream` (AccountStream degrades gracefully for tenants without an ACCOUNT egestion mapping) |
| `tap_cbx1/client.py` | `CBX1Stream` base class — keyset pagination, pinned windows, durable resume state. **Read the docstrings here before touching pagination/state.** |
| `tap_cbx1/auth.py` | `TapCBX1Auth` — access-key → JWT session token against CBX1 IDM |
| `tap_cbx1/schema_utils.py` | Dynamic schema discovery: CBX1 `flattenedJsonSchemaForJsonPath` → Singer schema |
| `tap_cbx1/constants.py` | Config key names, `DEFAULT_PAGE_SIZE` |
| `tests/test_filters.py` | pytest suite: request payload construction, cursor pagination, `updatedBy` filtering, state windowing |

CLI entry point (pyproject): `tap-cbx1 = 'tap_cbx1.tap:TapCBX1.cli'`.

## Config (`config.json`)

```json
{
    "Code": "…",
    "OrgId": "…",
    "CRMSystem": "SALESFORCE",
    "page_size": 500,
    "start_date": "2024-01-01T00:00:00.000Z"
}
```

- `Code` + `OrgId` (required): access-key credentials for CBX1 IDM, from the tenant's Descope access key setup (ask in #eng-crm-self-serve for QA-tenant credentials). The auth flow GETs `{BASE_URL}api/g/v1/auth/tokens` with `authenticationType=ACCESS_KEY` and receives a `sessionToken` (JWT, `maxAge` default 30 days). The token and `expires_in` are **written back into the config file** (`AccessToken` key) — write-only bookkeeping: nothing reads them back, and every fresh process re-authenticates (the token is only reused in-memory within one run). It's why local `config.json` files grow extra keys after a run; never commit those.
- `CRMSystem` (required at runtime): interpolated into both the list endpoint and the schema endpoint paths.
- `page_size` (optional, default 100): keyset pagination page size. Larger is cheap (no skip cost); HotGlue prod configs use 500.
- `start_date` (optional): initial lower bound for the replication window when no state exists.

## Environment variables

| Variable | Required | Purpose |
|---|---|---|
| `BASE_URL` | yes | CBX1 Java backend base URL, **with trailing slash** (code does `BASE_URL + "api/…"`). e.g. `http://java-backend.api.qa.cbx1.internal/` |
| `HOTGLUE_PRINCIPAL_ID` | no | UUID of HotGlue's CBX1 SERVICE_ACCOUNT for the deployment env. When set, records whose `updatedBy` equals this UUID are dropped **tap-side after fetch**, to avoid re-ingesting our own writes. Deliberately not pushed down as `$ne updatedBy` — that regresses Mongo on tenants where HotGlue is the dominant writer (verified via prod `explain()`). |

Copy `.env.example` to `.env` for local runs.

## API surface used

- Auth: `GET {BASE_URL}api/g/v1/auth/tokens?authenticationType=ACCESS_KEY&code=…&orgId=…`
- List (sync): `POST {BASE_URL}api/t/v1/targets/integrations/{TARGET}/{CRMSystem}/list?deanonymizePIIData=true` where `TARGET` ∈ `CONTACT`, `ACCOUNT`
- Schema (discover): `GET {BASE_URL}api/t/v1/targets/integrations/{target_name}/{CRMSystem}/jsonSchema`

## Pagination & state — the load-bearing design

**Keyset (cursor) pagination, never page-number/skip.** The backend returns an opaque `(updatedAt, id)` cursor; every page costs the same regardless of depth. Skip-based paging pins the prod Mongo primary at depth (CM100 timeouts). Payloads send `cursor` (empty string on the first page) — never `pageNumber`.

**Pinned windows.** Each run reads a window `(bookmark, window_end]` sorted `updatedAt DESC`, where `window_end` is pinned at the run's start (`now()`), and **reused on resume** — recomputing it would let new arrivals slip in at the top of a DESC scan and be skipped.

**Durable intra-run resume.** After each page's records are emitted, `(cursor, window_end)` is persisted to Singer state. A run that dies partway resumes from that cursor against the same window. On clean window completion the run-to-run watermark (`replication_key_value`) advances to the pinned upper bound and the cursor is cleared.

**Manual watermark.** The SDK's record-driven high-watermark is disabled (`_increment_stream_state` returns None) because it assumes ASC ordering; with DESC it would commit the newest `updatedAt` on a partial run and silently skip the unread tail.

**Fail-loud invariant.** A full page with no cursor and `last != true` raises `RuntimeError` instead of terminating: terminating would record false progress and drop the unread remainder (signature of a non-keyset backend build during a rolling deploy).

**AccountStream graceful degradation.** Tenants without an ACCOUNT egestion mapping: discovery falls back to a minimal schema; a 4xx on the *first* page with no prior progress yields zero records instead of failing. A 4xx *after* progress propagates (otherwise the persisted cursor would wedge every subsequent run).

## Running locally

See [Quickstart](#quickstart) for the discover → sync commands. Operational notes:

- **Logs go to stderr, Singer messages to stdout** — always redirect stdout to a file.
- **Incremental runs:** pass `--state state.json`. To capture next-run state, take the **last** STATE line from the previous output:
  ```bash
  grep '"type": "STATE"' output.singer | tail -1 | python3 -c 'import sys,json; print(json.dumps(json.load(sys.stdin)["value"]))' > state.json
  ```
- **Stream selection:** edit `catalog.json` metadata (`"selected": false`) to skip a stream.
- **Reading the output:** see [`docs/singer-format.md`](docs/singer-format.md) for the SCHEMA/RECORD/STATE primer and this tap's state semantics. Quick checks:
  ```bash
  grep -c '"type": "RECORD"' output.singer                      # record count
  grep '"type": "STATE"' output.singer | tail -1                # final state
  ```
- **End-to-end check with the target:**
  ```bash
  cat output.singer | (cd ../cbx1-target-hotglue && poetry run target-cbx1 --config config.json)
  ```
  Requires a QA-tenant config in the target repo — see that repo's README.

Local `catalog.json` / `output.singer` / `state.json` artifacts are gitignored (they can contain tenant data); an `output.singer` from a QA-tenant run makes a good local fixture for the `cbx1-target-hotglue` repo.

## Tests

```bash
poetry run pytest            # all
poetry run pytest tests/test_filters.py -k cursor   # focused
```

The suite covers: payload uses cursor not pageNumber, page_size defaulting, `testMetadata` filter always present, BETWEEN window construction, `HOTGLUE_PRINCIPAL_ID` skip behavior, resume-state handling.

## Debugging playbook

| Symptom | Likely cause / where to look |
|---|---|
| `Failed OAuth login` RuntimeError at startup | Bad `Code`/`OrgId`, or `BASE_URL` missing/lacking trailing slash. Check `auth.py::update_access_token`; the response body is included in the error. |
| `TypeError: unsupported operand … NoneType` mentioning `BASE_URL` | `BASE_URL` env var not set (both `auth.py` and `client.py` do `os.getenv("BASE_URL") + …`). |
| Discovery fails: `Failed to fetch schema for target …` | The tenant/CRM has no egestion mapping configured, or auth headers rejected. For ACCOUNT this degrades to a fallback schema; for CONTACT it raises. Check `schema_utils.fetch_schema_from_api` (expects status code `CM000` and `flattenedJsonSchemaForJsonPath` in `data[1]`). |
| `RuntimeError: … full page … no keyset cursor` | Backend not on the keyset-cursor build (rolling deploy/rollback). Do not "fix" the tap — deploy the backend. |
| Records missing between runs | Inspect the STATE messages in the output: is `replication_key_value` advancing past data that was never read? Check for a stale `cursor`/`window_end` pair in state. |
| Same records re-read every run | Window never completes (run always dies partway) — cursor persists but watermark never advances. Look for the failure that ends each run. |
| Our own writes echoing back into the pipeline | `HOTGLUE_PRINCIPAL_ID` unset or wrong UUID for the env. The skip count is logged at run end: `Skipped N records last-modified by HotGlue principal`. |
| CM100 / backend CPU spikes during sync | Something is sending page-number/skip pagination. This tap must always send `cursor`. |

When records go missing or duplicate, verify the three state-health invariants (full state semantics in [`docs/singer-format.md`](docs/singer-format.md)):

1. A clean run's final STATE has `replication_key_value` = the window upper bound and **no** `cursor`/`window_end` keys.
2. A mid-run STATE carrying `cursor` + `window_end` is a resume point — normal during a run, a wedge if it persists across runs.
3. Payloads must send `cursor`, never `pageNumber` (the tests enforce this; or log the payload from `prepare_request_payload`).

## Conventions

- Never widen the `$ne updatedBy` push-down or switch to skip pagination — both decisions are documented above and in code docstrings with prod evidence.
- Datetime fields (`createdAt`, `updatedAt`, `dataUpdatedAt`) are forced to `DateTimeType` in schema conversion regardless of what the API schema says.
- Nested field paths (`hqLocation.city`) are flattened with underscores (`hqLocation_city`); `[*]` array paths map to array-of-scalar types.

## Related repos

- [`cbx1-target-hotglue`](https://github.com/CBX1/cbx1-target-hotglue) — Singer target (write side)
- [`hotglue-transformation-scripts`](https://github.com/CBX1/hotglue-transformation-scripts) — ETL between tap and target; contains the end-to-end architecture doc
