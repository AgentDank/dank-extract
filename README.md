# AgentDank: &nbsp;&nbsp; `dank-extract`

`dank-extract` is a CLI tool for fetching, cleaning, and exporting cannabis datasets. It extracts data from public sources, applies cleaning and validation, and outputs to multiple formats (CSV, JSON, DuckDB).

The cleaned data snapshots are published to the [`dank-data`](https://github.com/AgentDank/dank-data) repository. These datasets power the [`dank-mcp`](https://github.com/AgentDank/dank-mcp) Model Context Protocol server.

----

  * [Installation](#installation)
  * [Usage](#usage)
  * [Supported Datasets](#supported-datasets)
  * [Data Cleaning](#data-cleaning)
  * [Building](#building)
  * [Contribution and Conduct](#contribution-and-conduct)
  * [Credits and License](#credits-and-license)

## Installation

Install using `go install`:

```sh
$ go install github.com/AgentDank/dank-extract@latest
```

It will be installed in your `$GOPATH/bin` directory, which is often `~/go/bin`.

## Usage

```
dank-extract [options]

Options:
  -c, --compress                 Compress output files with zstd
      --db string                DuckDB file path (default: .dank/dank-extract.duckdb)
  -h, --help                     Show help
      --max-cache-age duration   Maximum age of cached data before re-fetching (default 24h0m0s)
  -n, --no-fetch                 Don't fetch data, use existing cache
  -o, --output string            Output directory for exports (default: current directory)
      --root string              Root directory for .dank data (default ".")
  -t, --token string             ct.data.gov App Token
  -v, --verbose                  Verbose output
```

### Example

Fetch, clean, and export CT cannabis brand data:

```sh
$ dank-extract --verbose
2026/01/09 16:46:31 Fetching CT brands data...
2026/01/09 16:46:35 Fetched 30841 brands from API
2026/01/09 16:46:35 Cleaned brands: 30841 -> 30836 (removed 5 erroneous records)
2026/01/09 16:46:35 Wrote CSV to us_ct_brands.csv
2026/01/09 16:46:35 Wrote JSON to us_ct_brands.json
2026/01/09 16:46:41 Wrote DuckDB to .dank/dank-extract.duckdb
Successfully processed 30836 CT cannabis brands
```

### Output Files

- `us_ct_brands.csv` - CSV format with all fields
- `us_ct_brands.json` - JSON format with all fields
- `.dank/dank-extract.duckdb` - DuckDB database with indexed tables

Use `--compress` to output `.zst` compressed files.

## Supported Datasets

Currently the following datasets are supported:

 * [US CT Medical Marijuana and Adult Use Cannabis Brand Registry](https://data.ct.gov/Health-and-Human-Services/Medical-Marijuana-and-Adult-Use-Cannabis-Brand-Reg/egd5-wb6r/about_data)

## Data Cleaning

Because upstream datasets are not perfect, we apply cleaning:

- **Empty/Trace Values**: Fields with "TRC", "<LOQ", "<0.1", etc. are treated as trace amounts
- **Error Detection**: Multiple decimal points, invalid characters, letters at start
- **Validation**: Cannabinoid/terpene percentages must be 0-100%
- **Missing Data**: Empty brand names are filtered out

Generally, we remove weird characters and treat detected "trace" amounts as 0. We also remove rows with ridiculous data (e.g., 90,385% THC entries from decimal point errors).

## Building

Building is performed with standard Go tooling:

```sh
$ go build -o dank-extract ./cmd/dank-extract
```

Or using `go install`:

```sh
$ go install ./cmd/dank-extract
```

## Project Structure

```
dank-extract/
├── cmd/dank-extract/main.go    # CLI entry point
├── sources/
│   ├── cache.go                # Cache file management
│   └── us/ct/
│       ├── brand.go            # CT Brand struct, fetch, clean, export
│       └── measure.go          # Measure type with validation
├── internal/db/
│   ├── db.go                   # DuckDB utilities
│   └── duckdb_up.sql           # Schema migration
├── go.mod
└── go.sum
```

----

## Contribution and Conduct

Pull requests and issues are welcome. Or fork it. You do you.

Either way, obey our [Code of Conduct](./CODE_OF_CONDUCT.md). Be shady, but don't be a jerk.

## Credits and License

Copyright (c) 2025 Neomantra Corp. Authored by Evan Wies for [AgentDank](https://github.com/AgentDank).

Released under the [MIT License](https://en.wikipedia.org/wiki/MIT_License), see [LICENSE.txt](./LICENSE.txt).

----
Made with :herb: and :fire: by the team behind [AgentDank](https://github.com/AgentDank).
