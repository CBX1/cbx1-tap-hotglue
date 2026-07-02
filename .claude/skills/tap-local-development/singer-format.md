# Singer message format — quick primer (as used by tap-cbx1)

A Singer tap writes newline-delimited JSON messages to stdout. Three types matter:

## SCHEMA

Declares a stream's JSON Schema before its records. One per stream (this tap discovers schemas dynamically from the CBX1 `jsonSchema` endpoint).

```json
{"type": "SCHEMA", "stream": "contacts", "schema": {"properties": {"id": {"type": ["string"]}, "updatedAt": {"format": "date-time", "type": ["string"]}}}, "key_properties": ["id"]}
```

- `key_properties` = primary key (`id` for both streams here).

## RECORD

One data row.

```json
{"type": "RECORD", "stream": "contacts", "record": {"id": "…", "updatedAt": "2026-01-27T10:15:00.123Z", "email": "…"}}
```

## STATE

A checkpoint the runner (HotGlue) persists and passes back on the next run via `--state`.

```json
{"type": "STATE", "value": {"bookmarks": {"contacts": {"replication_key": "updatedAt", "replication_key_value": "2026-01-27T10:20:00.000000Z"}}}}
```

### tap-cbx1 state semantics

This tap manages state manually (see `AGENTS.md` → Pagination & state). Per stream bookmark:

| Key | Meaning |
|---|---|
| `replication_key_value` | Run-to-run watermark: lower bound of the next run's window. Advances **only on clean window completion**. |
| `cursor` | Intra-run keyset resume token. Present mid-run and after a partial run; absent after a clean run. |
| `window_end` | Pinned upper bound of the in-progress window. Always paired with `cursor`. |

Health rules:

- Clean run: final STATE has `replication_key_value` set, no `cursor`/`window_end`.
- Partial run: final STATE has `cursor` + `window_end` → next run resumes the same window (correct, by design).
- Corrupt/half state (`cursor` without `window_end` or vice versa) is discarded and a fresh window starts.

## Target side

A Singer target reads these messages on stdin, validates RECORDs against the SCHEMA, and emits its own final STATE to stdout when its writes are durable. That's why `tap | target > state.json` composes.
