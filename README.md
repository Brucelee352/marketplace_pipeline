# Marketplace Pipeline

This pipeline scores mental health coverage for individual health plans sold on the ACA Health Insurance Marketplace in twelve Florida counties. I wanted to know which plan would give me the best mental health coverage in the Florida metros I'd consider living in, so I picked counties where I've lived or that I liked visiting.

A Python script pulls 2025 plan data from the CMS Marketplace API and loads it into DuckDB. dbt transforms it into a fact table that scores each plan from 0 to 100 on mental health coverage, and a Dash app lets you explore and compare the results.

## Architecture

```
CMS Marketplace API
        |
        v
scripts/main.py          <- paginated POST /plans/search, twelve counties
        |
        +-- data/snapshots/     <- timestamped CSV artifacts per run
        +-- data/*.csv          <- latest CSVs (overwritten each run)
        |
        v
marketplace.duckdb
  +-- raw_data schema
        +-- plans
        +-- benefits
        +-- deductibles
        +-- moops
        +-- issuer
        +-- rating
        |
        v
dbt (marketplace_pipeline/)
  +-- marketplace.duckdb
        +-- snapshots.snp_plans   (SCD Type 2 plan history)
        +-- main.stg_*            (staging views)
        +-- main.dim_*            (mart tables)
        +-- main.fct_*            (fact tables)
        |
        v
app/app.py               <- Dash app, reads from main.dim_*, main.fct_*
```

### Tech stack

| Layer         | Tool                                      |
|---------------|-------------------------------------------|
| Extract       | Python `requests`, pagination             |
| Load          | `duckdb` Python driver                    |
| Transform     | `dbt-duckdb` 1.9+                         |
| Storage       | DuckDB 1.5+                               |
| Visualization | Dash 4.5+, Plotly 6, Dash AG Grid, Dash Bootstrap Components |
| Serving       | gunicorn in a Docker container            |
| Runtime       | Python 3.13, `uv`                         |

### Project structure

```
marketplace_pipeline/          <- repo root
+-- scripts/
|   +-- main.py                <- ETL entry point
+-- app/
|   +-- app.py                 <- Dash visualization app
+-- data/
|   +-- *.csv                  <- latest snapshot (overwritten each run)
|   +-- snapshots/             <- timestamped CSVs for audit trail
+-- logs/
|   +-- pipeline.log           <- run logs
+-- marketplace.duckdb         <- source database (raw_data schema)
+-- marketplace_pipeline/      <- dbt project
|   +-- docs/                  <- dbt docs overview page
|   +-- models/
|   |   +-- sources.yml
|   |   +-- staging/
|   |   +-- marts/
|   |   +-- facts/
|   +-- snapshots/
|   +-- dbt_project.yml
|   +-- packages.yml
+-- Dockerfile                 <- app image (gunicorn)
+-- pyproject.toml
+-- .env                       <- API credentials (not committed)
```

### Counties covered

| County FIPS | County       | ZIP Code |
|-------------|--------------|----------|
| 12083       | Marion       | 34470    |
| 12057       | Hillsborough | 33602    |
| 12011       | Broward      | 33301    |
| 12095       | Orange       | 32801    |
| 12103       | Pinellas     | 33701    |
| 12031       | Duval        | 32202    |
| 12115       | Sarasota     | 34236    |
| 12099       | Palm Beach   | 33401    |
| 12069       | Lake         | 34748    |
| 12001       | Alachua      | 32601    |
| 12105       | Polk         | 33801    |
| 12086       | Miami-Dade   | 33128    |

## Setup

### 1. Install dependencies

```bash
uv sync
```

### 2. Configure environment

Create a `.env` file at the repo root:

```
API_KEY=<your CMS Marketplace API key>
BASE_URL=https://marketplace.api.healthcare.gov/api/v1
LOG_LEVEL=INFO
```

You can get a free API key at [healthcare.gov/developers](https://developer.healthcare.gov/). The counties live in the `COUNTIES` dictionary in `scripts/main.py`, so they don't go in `.env`.

### 3. Configure the dbt profile

The dbt profile lives at `~/.dbt/profiles.yml`. The pipeline uses the `dev` target, which points at `marketplace.duckdb`:

```yaml
marketplace_pipeline:
  outputs:
    dev:
      type: duckdb
      path: ../marketplace.duckdb
      threads: 1
  target: dev
```

## Running the pipeline

```bash
# Full run: extract -> load -> transform
uv run python scripts/main.py
```

The script runs three steps in order:

1. Extract: pages through `POST /plans/search` on the Marketplace API and collects plans, benefits, deductibles, MOOPs, issuer info, and CMS quality ratings for every county.
2. Load: writes each dataset to a table in the `raw_data` schema of `marketplace.duckdb`, and saves timestamped CSVs to `data/snapshots/` for auditing.
3. Transform: runs `dbt deps` and then `dbt build --target dev` inside `marketplace_pipeline/`. `dbt build` runs the snapshot, the staging, mart, and fact models, and the tests in dependency order.

### Run dbt on its own

```bash
cd marketplace_pipeline
dbt snapshot --target dev   # run first: captures plan history
dbt run --target dev
dbt test --target dev
```

### Launch the Dash app

```bash
uv run python app/app.py
```

The app opens at `http://localhost:8050`. Set `PORT` or `HOST` to change where it listens, and `DASH_DEBUG=1` to turn on hot reload. It reads the dbt models straight from `marketplace.duckdb` in read-only mode (`main.dim_plan`, `main.fct_plan_benefits`, `main.fct_plan_mh_coverage_score`).

In production the `Dockerfile` copies the app, the database, and the built dbt docs into a `python:3.12-slim` image and serves `app.app:server` with gunicorn on `$PORT`.

## How plans are scored

`fct_plan_mh_coverage_score` gives each county and plan pair a score out of 100:

| Points | Component                              | Scale                     |
|--------|----------------------------------------|---------------------------|
| 40     | In-network outpatient therapy copay    | $0 is best, $125 scores 0 |
| 25     | In-network individual deductible       | $0 is best, $9,200 scores 0 |
| 20     | Mental health benefits covered         | out of 2                  |
| 15     | CMS quality rating                     | 1 to 5 stars              |

A plan with no deductible on file gets $9,200, the highest deductible in this market. A plan with no rating gets 0 points for that component. In the app, plans that charge coinsurance instead of a copay are costed at an assumed $150 per therapy session.

## Raw data tables

All tables live in the `raw_data` schema of `marketplace.duckdb`. The extraction loop adds `county_fips` and `county_name` as the first two columns of every table.

| Table         | Grain                                             | Key columns                                                       |
|---------------|---------------------------------------------------|-------------------------------------------------------------------|
| `plans`       | One row per county x plan                         | `county_fips`, `id`, `name`, `premium`, `metal_level`, `type`    |
| `benefits`    | One row per county x plan x benefit x cost-share  | `county_fips`, `plan_id`, `benefit_type`, `network_tier`, `copay` |
| `deductibles` | One row per county x plan x deductible type       | `county_fips`, `plan_id`, `type`, `network_tier`, `amount`        |
| `moops`       | One row per county x plan x MOOP type             | `county_fips`, `plan_id`, `type`, `network_tier`, `amount`, `csr` |
| `issuer`      | One row per county x plan (carrier info)          | `county_fips`, `plan_id`, `issuer_id`, `name`                     |
| `rating`      | One row per county x plan (CMS quality ratings)   | `county_fips`, `plan_id`, `global_rating`                         |

## Fixed bugs and leftover data

Two extraction bugs have been fixed:

- `ratings` and `issuer` rows were appended inside the `for moop` loop, which produced one row per MOOP per plan. They're now appended once per plan.
- The block that collects benefits, deductibles, and MOOPs ran inside the `while True` pagination loop, so every new page reprocessed the plans from earlier pages. It now runs after the loop exits.

The current DuckDB snapshot still holds data from before the fixes:

- `benefits` has about 7.8 times as many rows as it should. `stg_benefits` removes the duplicates with `SELECT DISTINCT`.
- `issuer` and `rating` each have about 7.5 times as many rows as they should. `stg_issuer` and `stg_rating` collapse them with `GROUP BY`.
- Nine plans with `$0` deductibles have no issuer or rating row. They still appear in every fact table through a `LEFT JOIN`, and their carrier name falls back to the CMS issuer ID prefix.

The next full pipeline run replaces this data.

## Outputs

After a successful run, `marketplace.duckdb` contains:

- `raw_data.*`: the six source tables as loaded from the API
- `main.stg_*`: six cleaned staging views
- `main.dim_issuer`: one row per insurance carrier
- `main.dim_plan`: one row per county and plan, with carrier and quality rating attributes. The surrogate key is `plan_key = md5(county_fips || '|' || plan_id)`.
- `main.fct_plan_benefits`: one row per county, plan, benefit type, and network tier
- `main.fct_plan_mh_coverage_score`: one row per county and plan, with the 0 to 100 mental health coverage score

See [`marketplace_pipeline/README.md`](marketplace_pipeline/README.md) for the full model reference.
