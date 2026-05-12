# Marketplace Pipeline

An end-to-end data pipeline that evaluates mental health service coverage across individual health insurance plans listed on the US Public Health Insurance Marketplace for Marion County, FL (FIPS 12083).

The pipeline extracts plan data from the CMS Marketplace API, loads it into DuckDB, transforms it with dbt, and produces a scored fact table ranking each plan by the quality of its mental health benefit coverage.

---

## Architecture

```
CMS Marketplace API
        │
        ▼
scripts/main.py          ← paginated POST /plans/search
        │
        ├── data/snapshots/          ← timestamped CSV artifacts per run
        ├── data/*.csv               ← latest CSVs (overwritten each run)
        │
        ▼
marketplace.duckdb
  └── raw_data schema
        ├── plans
        ├── benefits
        ├── deductibles
        ├── moops
        ├── issuer
        └── rating
        │
        ▼
dbt (marketplace_pipeline/)
  └── marketplace.duckdb
        ├── staging views   (stg_*)
        ├── marts tables    (dim_*)
        └── facts tables    (fct_*)
```

---

## Tech Stack

| Layer       | Tool                          |
|-------------|-------------------------------|
| Extract     | Python `requests`, pagination |
| Load        | `duckdb` Python driver        |
| Transform   | `dbt-duckdb` 1.9+             |
| Storage     | DuckDB 1.5+                   |
| Validation  | `pandera` (schema guards)     |
| Runtime     | Python 3.13, `uv`             |

---

## Project Structure

```
marketplace_pipeline/          ← repo root
├── scripts/
│   └── main.py                ← ETL entry point
├── data/
│   ├── *.csv                  ← latest snapshot (overwritten each run)
│   └── snapshots/             ← timestamped CSVs for audit trail
├── logs/
│   └── pipeline.log           ← run logs
├── marketplace.duckdb         ← source database (raw_data schema)
├── marketplace_pipeline/      ← dbt project
│   ├── models/
│   │   ├── sources.yml
│   │   ├── staging/
│   │   ├── marts/
│   │   └── facts/
│   ├── dbt_project.yml
│   └── packages.yml
├── pyproject.toml
└── .env                       ← API credentials (not committed)
```

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
FIPS=12083
STATE=FL
ZIP=34471
LOG_LEVEL=INFO
```

Get a free API key at [healthcare.gov/developers](https://developer.healthcare.gov/).

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
# Full run: extract → load → transform
uv run python scripts/main.py
```

This does three things in sequence:

1. **Extract** — pages through the Marketplace API (`POST /plans/search`) collecting plans, benefits, deductibles, MOOPs, issuer info, and CMS quality ratings for the configured county and ZIP code.
2. **Load** — writes each dataset as a table in the `raw_data` schema of `marketplace.duckdb`. Also saves timestamped CSVs to `data/snapshots/` for auditing.
3. **Transform** — runs `dbt clean → dbt deps → dbt run` inside `marketplace_pipeline/`, building the full staging → marts → facts model DAG.

### Run dbt independently

```bash
cd marketplace_pipeline
dbt run --target dev
dbt test --target dev
```

---

## Raw Data Tables

All tables live in the `raw_data` schema of `marketplace.duckdb`.

| Table        | Grain                                    | Key columns                                                         |
|--------------|------------------------------------------|---------------------------------------------------------------------|
| `plans`      | One row per plan                         | `id`, `name`, `premium`, `metal_level`, `type`, `design_type`       |
| `benefits`   | One row per plan × benefit × cost-share  | `plan_id`, `benefit_type`, `network_tier`, `copay`, `coinsurance_rate` |
| `deductibles`| One row per plan × deductible type       | `plan_id`, `type`, `network_tier`, `amount`, `family_cost`          |
| `moops`      | One row per plan × MOOP type             | `plan_id`, `type`, `network_tier`, `amount`, `csr`                  |
| `issuer`     | One row per plan (carrier info)          | `plan_id`, `issuer_id`, `name`, `state`                             |
| `rating`     | One row per plan (CMS quality ratings)   | `plan_id`, `global_rating`, `enrollee_experience_rating`            |

---

## Data Quality Notes

**Extraction bugs fixed in this version:**

- `ratings` and `issuer` rows were previously accumulated inside the `for moop` loop, producing one row per MOOP per plan instead of one per plan. Fixed — both are now appended once per plan.
- The benefit/deductible/MOOP accumulation block was previously inside the `while True` pagination loop, causing all prior pages' plans to be reprocessed on each new page. Fixed — accumulation now runs after the loop exits.

**Residual effects in the current DuckDB snapshot** (pre-fix data):

- `benefits` contains ~7.8× duplicate rows — handled by `SELECT DISTINCT` in `stg_benefits`.
- `issuer` and `rating` each have ~7.5× duplicate rows — handled by `GROUP BY` in `stg_issuer` and `stg_rating`.
- 9 plans with `$0` deductibles have no issuer or rating row — these plans still appear in all fact tables via `LEFT JOIN`; carrier name falls back to the CMS issuer ID prefix.

All of the above self-heal on the next full pipeline run.

---

## Outputs

After a successful run, `marketplace.duckdb` contains:

- `raw_data.*` — six source tables as loaded from the API
- `main.stg_*` — six cleaned staging views
- `main.dim_plan` — one row per plan with carrier and quality rating attributes
- `main.dim_issuer` — one row per insurance carrier
- `main.fct_plan_benefits` — one row per plan × benefit type × network tier
- `main.fct_plan_mh_coverage_score` — one row per plan, scored 0–100 for mental health coverage quality

See [`marketplace_pipeline/README.md`](marketplace_pipeline/README.md) for the full model reference.
