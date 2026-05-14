{% docs __overview__ %}

# Florida Marketplace — Mental Health Coverage Pipeline

An end-to-end ELT pipeline that evaluates mental health service coverage across
individual health insurance plans listed on the US Public Health Insurance
Marketplace for four Florida counties I have previously resided in: **Marion,
Hillsborough, Broward, and Orange**.

The pipeline extracts plan data from the CMS Marketplace API, loads it into
DuckDB, and transforms it with dbt to produce a scored fact table ranking each
plan by the quality of its mental health benefit coverage.

---

## Architecture

```
CMS Marketplace API  (POST /plans/search, paginated)
        │
        ▼
scripts/main.py
        ├── data/snapshots/     ← timestamped CSV artifacts per run
        └── data/*.csv          ← latest CSVs (overwritten each run)
        │
        ▼
marketplace.duckdb  →  raw_data schema
        ├── plans
        ├── benefits
        ├── deductibles
        ├── moops
        ├── issuer
        └── rating
        │
        ▼
dbt (this project)  →  marketplace.duckdb
        ├── snapshots.snp_plans   (SCD Type 2 plan history)
        ├── main.stg_*            (staging views — clean & deduplicate)
        ├── main.dim_*            (mart tables — conformed dimensions)
        └── main.fct_*            (fact tables — scored outputs)
```

---

## Model DAG

```
raw_data (DuckDB schema)
│
├── snp_plans  (snapshots — SCD Type 2, check strategy)
│
├── stg_plans ─────────────────────────────────┐
├── stg_benefits ─────────────────────────┐    │
├── stg_deductibles ──────────────────┐   │    ├── dim_plan ──┐
├── stg_moops ─────────────────────┐  │   │    │              │
├── stg_issuer ────────────────────┼──┼───┘    └── dim_issuer │
└── stg_rating ────────────────────┘  │                       │
                                      │                       │
                                      └───────────────────────┼── fct_plan_mh_coverage_score
                                                              │
                                      stg_benefits ───────────┴── fct_plan_benefits
```

| Layer      | Materialization | Purpose                                    |
|------------|-----------------|--------------------------------------------|
| `staging/` | view            | Clean, cast, and deduplicate raw sources   |
| `marts/`   | table           | Conformed dimensions ready for reuse       |
| `facts/`   | table           | Business metrics and scored outputs        |

---

## Counties Covered

| County FIPS | County       | ZIP Code |
|-------------|--------------|----------|
| 12083       | Marion       | 34470    |
| 12057       | Hillsborough | 33602    |
| 12011       | Broward      | 33301    |
| 12095       | Orange       | 32801    |

---

## Coverage Score Formula

The primary analytical output (`fct_plan_mh_coverage_score`) scores each plan
0–100 across four dimensions of mental health coverage quality:

```
coverage_score =
    (mh_benefits_covered / 2)  × 20   -- MH coverage completeness   (0–20 pts)
  + (1 − avg_mh_copay / 125)   × 40   -- Outpatient copay efficiency (0–40 pts)
  + (1 − deductible / 9200)    × 25   -- Deductible access           (0–25 pts)
  + (global_rating / 5)        × 15   -- CMS quality rating          (0–15 pts)
```

Calibrated to the 2025 Florida individual market: max observed outpatient copay
$125, ACA statutory deductible maximum $9,200, CMS QRS scale 1–5 stars.

---

## Data Quality Notes

Two extraction bugs were identified and fixed in `scripts/main.py`:

- **Accumulation inside pagination loop** — benefit/deductible/MOOP rows were
  reprocessed on every new page fetch, producing ~7.8× duplicate rows in the
  `benefits` raw table. Fixed; residual duplicates absorbed by `SELECT DISTINCT`
  in `stg_benefits`.

- **Ratings/issuers inside the MOOP loop** — issuer and rating rows were
  appended once per MOOP row instead of once per plan, producing ~7.5× duplicate
  rows. Fixed; absorbed by `GROUP BY` in `stg_issuer` and `stg_rating`.

All residual effects self-heal on the next full pipeline run.

---

## Tech Stack

| Layer     | Tool                     |
|-----------|--------------------------|
| Extract   | Python `requests`        |
| Load      | `duckdb` Python driver   |
| Transform | `dbt-duckdb` 1.9+        |
| Storage   | DuckDB 1.5+              |
| Runtime   | Python 3.13, `uv`        |

{% enddocs %}
