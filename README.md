# tap-cbx1

Singer tap that extracts CRM data (accounts, contacts) from the CBX1 platform API. Built with the [Meltano Singer SDK](https://sdk.meltano.com/). Runs inside HotGlue as the source side of the CBX1 ↔ CRM sync pipeline.

> **Agents / detailed reference:** see [`AGENTS.md`](AGENTS.md) for architecture, pagination/state semantics, and debugging guidance.

## Quickstart

```bash
poetry install
```

Create a `config.json`:

```json
{
    "Code": "<access key code from CBX1 IDM>",
    "OrgId": "<tenant organization id>",
    "CRMSystem": "<crm system, e.g. SALESFORCE or HUBSPOT>",
    "page_size": 500,
    "start_date": "2024-01-01T00:00:00.000Z"
}
```

| Field | Required | Purpose |
|---|---|---|
| `Code` | yes | Access-key code used against CBX1 IDM (`/api/g/v1/auth/tokens`) to obtain a JWT session token |
| `OrgId` | yes | Tenant organization id the access key belongs to |
| `CRMSystem` | yes (runtime) | CRM the egestion mapping is configured for; part of the list/schema endpoint paths |
| `page_size` | no | Records per page for keyset pagination (default 100) |
| `start_date` | no | Initial replication watermark when no state exists |

Set environment variables (see `.env.example`):

```bash
export BASE_URL="http://java-backend.api.qa.cbx1.internal/"   # trailing slash required
export HOTGLUE_PRINCIPAL_ID="<uuid>"                          # optional, see AGENTS.md
```

Discover, then sync:

```bash
poetry run tap-cbx1 --config config.json --discover > catalog.json
poetry run tap-cbx1 --config config.json --catalog catalog.json > output.singer
```

## Streams

- `contacts` (`/CONTACT`)
- `accounts` (`/ACCOUNT`) — degrades gracefully when a tenant has no ACCOUNT egestion mapping

Schemas are discovered dynamically from the CBX1 `jsonSchema` endpoint per stream.

## Tests

```bash
poetry run pytest
```

## Related repos

- [`cbx1-target-hotglue`](https://github.com/CBX1/cbx1-target-hotglue) — Singer target (write side)
- [`hotglue-transformation-scripts`](https://github.com/CBX1/hotglue-transformation-scripts) — ETL between tap and target; contains the end-to-end architecture doc
