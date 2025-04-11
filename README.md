# Tap for CBX1


## Required Config Fields

```
{
    "access_key": "...",
    "organization_id": "...",
    "user_id": "..."
}
```

## Authentication

This tap uses JWT authentication with CBX1's IDM. The tap will:

1. Fetch a JWT token using the provided access_key and organization_id
2. Use this JWT token for all API calls
3. Monitor token expiration and automatically refresh when needed (typically ~30 days)

## Streams

Current supported streams are:
- Accounts
- Contacts

## Schemas

The stream schemas are hardcoded in each individual stream class

## Running this locally

1. Install dependencies with Poetry:
```
poetry install
```

2. Create a config.json with the required fields

3. Run a discover to generate a catalog.json

```
tap-cbx1 --config <path to config> --discover > <desired location for catalog>
```

4. Run a sync against the catalog.json

```
tap-cbx1 --config <path to config> --catalog <path to catalog> > <path to data output>
```
