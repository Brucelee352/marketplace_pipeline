# Marketplace Pipeline

An end-to-end data pipeline that evaluates mental health service coverage across individual health insurance plans listed on the US Public Health Insurance Marketplace for several Florida counties. 

The reason being that I wanted to answer the question, "what is the best possible coverage that I could get that offers mental health services within popular Florida metros? The counties themselves that I had chosen are places that I've either lived in or liked visiting before in FL. (Don't ask me why Miami-Dade is there though haha)

The pipeline extracts plan data from the CMS Marketplace API, loads it into DuckDB, transforms it with dbt, and produces a scored fact table ranking each plan by the quality of its mental health benefit coverage. An interactive Dash app serves the results for exploration and comparison.

---

## Architecture

```
CMS Marketplace API
        |
        v
scripts/main.py          <- paginated POST /plans/search, four counties
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

---

## Tech Stack

| Layer         | Tool                          |
|---------------|-------------------------------|
| Extract       | Python `requests`, pagination |
| Load          | `duckdb` Python driver        |
| Transform     | `dbt-duckdb` 1.9+             |
| Storage       | DuckDB 1.5+                   |
| Visualization | Dash 2.18+, Plotly 5.24+      |
| Runtime       | Python 3.13, `uv`             |

---

## Project Structure

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
+-- pyproject.toml
+-- .env                       <- API credentials (not committed)
```

---

## Counties Covered

| County FIPS | County       | ZIP Code |
|-------------|--------------|----------|
| 12083       | Marion       | 34470    |
| 12057       | Hillsborough | 33602    |
| 12011       | Broward      | 33301    |
| 12095       | Orange       | 32801    |
| 12103       | Pinellas     | 33701    |
| 12031       | Duval        | 32202    |
| 12081       | Manatee      | 34205    |
| 12115       | Sarasota     | 34236    |
| 12099       | Palm Beach   | 33401    |
| 12069       | Lake         | 34748    |
| 12001       | Alachua      | 32601    |
| 12105       | Polk         | 33801    |


---

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

Get a free API key at [healthcare.gov/developers](https://developer.healthcare.gov/). The four counties are configured directly in `scripts/main.py` as the `COUNTIES` dictionary and do not need to be set in `.env`.

### 3. Configure dbt profile

The dbt profile lives at `~/.dbt/profiles.yml`. The pipeline uses the `dev` target which points at `marketplace.duckdb`:

```yaml
marketplace_pipeline:
  outputs:
    dev:
      type: duckdb
      path: ../marketplace.duckdb
      threads: 1
  target: dev
```

---

## Running the Pipeline

```bash
# Full run: extract -> load -> transform
uv run python scripts/main.py
```

This does three things in sequence:

1. **Extract** -- pages through the Marketplace API (`POST /plans/search`) collecting plans, benefits, deductibles, MOOPs, issuer info, and CMS quality ratings for all counties.
2. **Load** -- writes each dataset as a table in the `raw_data` schema of `marketplace.duckdb`. Also saves timestamped CSVs to `data/snapshots/` for auditing.
3. **Transform** -- runs `dbt clean -> dbt deps -> dbt snapshot -> dbt run` inside `marketplace_pipeline/`, building the full staging -> marts -> facts model DAG.

### Run dbt independently

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

Opens at `http://localhost:8050`. Reads directly from `marketplace.duckdb` via the dbt-built models (`main.dim_plan`, `main.fct_plan_benefits`, `main.fct_plan_mh_coverage_score`).

---

## Raw Data Tables

All tables live in the `raw_data` schema of `marketplace.duckdb`. Every table includes `county_fips` and `county_name` as the first two columns, added during the multi-county extraction loop.

| Table         | Grain                                             | Key columns                                                       |
|---------------|---------------------------------------------------|-------------------------------------------------------------------|
| `plans`       | One row per county x plan                         | `county_fips`, `id`, `name`, `premium`, `metal_level`, `type`    |
| `benefits`    | One row per county x plan x benefit x cost-share  | `county_fips`, `plan_id`, `benefit_type`, `network_tier`, `copay` |
| `deductibles` | One row per county x plan x deductible type       | `county_fips`, `plan_id`, `type`, `network_tier`, `amount`        |
| `moops`       | One row per county x plan x MOOP type             | `county_fips`, `plan_id`, `type`, `network_tier`, `amount`, `csr` |
| `issuer`      | One row per county x plan (carrier info)          | `county_fips`, `plan_id`, `issuer_id`, `name`                     |
| `rating`      | One row per county x plan (CMS quality ratings)   | `county_fips`, `plan_id`, `global_rating`                         |

---

## Prior Bugs

- `ratings` and `issuer` rows were previously accumulated inside the `for moop` loop, producing one row per MOOP per plan instead of one per plan. Fixed -- both are now appended once per plan.
- The benefit/deductible/MOOP accumulation block was previously inside the `while True` pagination loop, causing all prior pages' plans to be reprocessed on each new page. Fixed -- accumulation now runs after the loop exits.

Residual effects in the current DuckDB snapshot (pre-fix data):

- `benefits` contains ~7.8x duplicate rows, handled by `SELECT DISTINCT` in `stg_benefits`.
- `issuer` and `rating` each have ~7.5x duplicate rows, handled by `GROUP BY` in `stg_issuer` and `stg_rating`.
- 9 plans with `$0` deductibles have no issuer or rating row. These plans still appear in all fact tables via `LEFT JOIN`, with carrier name falling back to the CMS issuer ID prefix.

All of the above self-heal on the next full pipeline run.

---

## Outputs

After a successful run, `marketplace.duckdb` contains:

- `raw_data.*` -- six source tables as loaded from the API
- `main.stg_*` -- six cleaned staging views
- `main.dim_issuer` -- one row per insurance carrier
- `main.dim_plan` -- one row per county x plan, with carrier and quality rating attributes; `plan_key = md5(county_fips || '|' || plan_id)` is the surrogate PK
- `main.fct_plan_benefits` -- one row per county x plan x benefit type x network tier
- `main.fct_plan_mh_coverage_score` -- one row per county x plan, scored 0-100 for mental health coverage quality

See [`marketplace_pipeline/README.md`](marketplace_pipeline/README.md) for the full model reference.
