# Additional CT Cannabis Data Sources — Design

Date: 2026-04-17
Status: Draft

## Summary

Add three CT cannabis datasets from data.ct.gov to `dank-extract`, following the existing per-dataset file pattern in `sources/us/ct/`:

| Dataset ID    | Title                                              | New file                 | CLI name          |
|---------------|----------------------------------------------------|--------------------------|-------------------|
| `42yd-3x3d`   | Licensed Cannabis and Medical Marijuana Retail Locations | `retail_location.go` | `retail-locations` |
| `khc7-gd9u`   | Cannabis Zoning                                    | `zoning.go`              | `zoning`          |
| `w85q-8cfm`   | Weekly Cannabis Establishment Lottery Report       | `lottery.go`             | `lottery`         |

Explicitly excluded:

- `p4ks-rfxp` — confirmed via Socrata metadata to be a map visualization view of `42yd-3x3d` (same underlying data).
- `f382-bnu5` — confirmed via Socrata metadata to be a chart visualization view of `ucaf-96h6` (same underlying weekly sales data).
- Pesticide guide lists (`8xsj-gz6v`, `b8ki-p9ef`, `crm6-xdta`) — out of scope per user direction.

## Source schemas

Schemas below are taken from live samples of the Socrata JSON API.

### `42yd-3x3d` — Retail Locations

```json
{
  "type": "Hybrid Retailer",
  "business": "FFD WEST LLC",
  "dba": "FINE FETTLE DISPENSARY - STAMFORD",
  "license": "AMHF.0008247",
  "street": "12 RESEARCH DR",
  "city": "STAMFORD",
  "zipcode": "06906-1419",
  "website": { "url": "https://www.finefettle.com/" },
  "location": { "type": "Point", "coordinates": [-73.52019, 41.07777] }
}
```

Notes:
- `website` is an object with a single `url` key, or omitted entirely (nullable).
- `location` is a GeoJSON Point (`[longitude, latitude]`). Per user direction, the raw GeoJSON object is preserved end-to-end so the DuckDB spatial extension can consume it.

### `khc7-gd9u` — Cannabis Zoning

```json
{ "town": "Newington", "state": "CT", "status": "Approved" }
```

### `w85q-8cfm` — Lottery Report

```json
{
  "credential_type": "ACDS: ADULT-USE CANNABIS DELIVERY SERVICE",
  "social_equity_council_lottery": 2,
  "general_lottery": 0
}
```

Following the convention established by `credential.go`, `sales.go`, and `tax.go`, all Socrata scalar values are stored as Go `string` fields regardless of their underlying type. Numeric access is via typed accessor methods (`FooInt()`, `FooFloat64()`). This matches how Socrata delivers JSON (quoted strings) and avoids unmarshal failures when fields are absent.

## Go structures

Each dataset gets one file under `sources/us/ct/`. File shape mirrors `credential.go`:

1. Filename constants (`*JSONFilename`, `*CSVFilename`, `*URL`).
2. Struct with JSON tags matching the Socrata field names.
3. `SocrataConfig` var + `Fetch*` wrapper around `sources.FetchSocrata[T]`.
4. `CSVHeaders()` / `CSVValue()` methods to satisfy `sources.CSVExportable`.
5. `DBInsert*` function that clears and reloads the table (snapshot semantics).

### `retail_location.go`

Struct:

```go
type RetailLocation struct {
    Type     string           `json:"type"`
    Business string           `json:"business"`
    DBA      string           `json:"dba"`
    License  string           `json:"license"`
    Street   string           `json:"street"`
    City     string           `json:"city"`
    Zipcode  string           `json:"zipcode"`
    Website  *RetailWebsite   `json:"website,omitempty"`
    Location *RetailLocationPoint `json:"location,omitempty"`
}

type RetailWebsite struct {
    URL string `json:"url"`
}

type RetailLocationPoint struct {
    Type        string    `json:"type"`
    Coordinates []float64 `json:"coordinates"` // [lon, lat]
}
```

CSV export: website is written as its `url` string (empty if nil). Location is written as two columns `longitude`, `latitude` (empty if nil) — CSV can't carry nested structure, and this keeps CSV consumable without reaching into GeoJSON.

JSON export: passthrough — the nested `website` object and `location` GeoJSON Point are preserved exactly as received.

### `zoning.go`

```go
type Zoning struct {
    Town   string `json:"town"`
    State  string `json:"state"`
    Status string `json:"status"`
}
```

### `lottery.go`

```go
type Lottery struct {
    CredentialType              string `json:"credential_type"`
    SocialEquityCouncilLottery  string `json:"social_equity_council_lottery"`
    GeneralLottery              string `json:"general_lottery"`
}
```

Typed accessors `SocialEquityCouncilLotteryInt()` / `GeneralLotteryInt()` via `strconv.Atoi`, matching the `Credential.CountInt()` convention.

## DuckDB schema

Append three `CREATE TABLE IF NOT EXISTS` blocks to `sources/us/ct/duckdb_up.sql`:

```sql
-- Retail Locations
CREATE TABLE IF NOT EXISTS ct_retail_locations (
    type       TEXT,
    business   TEXT,
    dba        TEXT,
    license    TEXT NOT NULL,
    street     TEXT,
    city       TEXT,
    zipcode    TEXT,
    website    TEXT,
    longitude  DOUBLE,
    latitude   DOUBLE
);
CREATE UNIQUE INDEX IF NOT EXISTS ct_retail_locations_license ON ct_retail_locations (license);
CREATE INDEX IF NOT EXISTS ct_retail_locations_city ON ct_retail_locations (city);
CREATE INDEX IF NOT EXISTS ct_retail_locations_type ON ct_retail_locations (type);

-- Zoning
CREATE TABLE IF NOT EXISTS ct_zoning (
    town    TEXT NOT NULL,
    state   TEXT,
    status  TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS ct_zoning_town ON ct_zoning (town);
CREATE INDEX IF NOT EXISTS ct_zoning_status ON ct_zoning (status);

-- Lottery
CREATE TABLE IF NOT EXISTS ct_lottery (
    credential_type              TEXT NOT NULL,
    social_equity_council_lottery INTEGER,
    general_lottery              INTEGER
);
CREATE UNIQUE INDEX IF NOT EXISTS ct_lottery_type ON ct_lottery (credential_type);
```

DuckDB storage choice for retail locations: flatten `longitude` / `latitude` to scalar columns. Reasoning: DuckDB's spatial extension can reconstruct a `POINT` from scalar columns cheaply (`ST_Point(longitude, latitude)`), and scalar columns are queryable without loading the extension. The lossless GeoJSON remains in the JSON export.

Note on the zoning unique index: CT has 169 towns; the current sample shows one row per town, so `town` is a reasonable unique key. If the upstream dataset ever emits duplicates (e.g., historical revisions), the `DBInsert*` function's `DELETE` + `INSERT` pattern will still work — the unique index simply enforces current-state assumptions.

## CLI wiring

In `cmd/dank-extract/main.go`:

1. Extend `availableDatasets` with `"retail-locations"`, `"zoning"`, `"lottery"`.
2. Register three new processors in the `processors` map: `processRetailLocations`, `processZoning`, `processLottery`. Each mirrors `processCredentials` (no cleaning step — these datasets don't need the kind of error-filtering `processBrands` does).
3. Update the `--dataset` help string.

## Output files

Matching the `us_ct_<dataset>.{csv,json}` convention:

- `us_ct_retail_locations.csv` / `us_ct_retail_locations.json`
- `us_ct_zoning.csv` / `us_ct_zoning.json`
- `us_ct_lottery.csv` / `us_ct_lottery.json`

## README and data_sources.json

- Add the three datasets under "Supported Datasets" in `README.md`.
- No update needed to `etc/data_sources.json` — its CT entry already describes data.ct.gov broadly; the file is a survey, not a per-dataset registry.

## Testing

No unit tests exist in the repo today; none are added. Verification is by running:

```
dank-extract --dataset retail-locations,zoning,lottery --verbose
```

and confirming: (1) files land in the output directory, (2) DuckDB tables are populated, (3) record counts look reasonable (retail ≈ dozens, zoning ≈ 169, lottery ≈ small).

## Out of scope

- Data cleaning beyond trimming whitespace. The upstream data for these three datasets appears clean; if problems emerge later, they can be addressed in a targeted pass.
- Snapshot consolidation / changes to `snapshots/us/ct/` layout. New datasets will flow through the existing snapshot mode unchanged.
- Changes to the sales datasets (`ucaf-96h6`). The investigation confirmed `f382-bnu5` is derived, so no action needed.
