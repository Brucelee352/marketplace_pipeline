# marketplace_pipeline — dbt Project

This dbt project transforms raw Marketplace API data into a clean dimensional model and a scored mental health coverage fact table. It connects to `marketplace.duckdb` (written by `scripts/main.py`) and builds all models in the same database.

---

## Connection

Profile: `marketplace_pipeline` (configured in `~/.dbt/profiles.yml`)  
Target: `dev` → `../marketplace.duckdb` (relative to this directory)  
Adapter: `dbt-duckdb`

```bash
dbt run --target dev
dbt test --target dev
```

---

## Model DAG

```
raw_data (DuckDB schema — written by main.py)
│
├── stg_plans ──────────────────────────────┐
├── stg_benefits ──────────────────────┐    │
├── stg_deductibles ───────────────┐   │    ├── dim_plan ──┐
├── stg_moops ─────────────────┐   │   │    │              │
├── stg_issuer ────────────────┼───┼───┘    └── dim_issuer │
└── stg_rating ────────────────┘   │                       │
                                   │                       │
                                   └───────────────────────┼── fct_plan_mh_coverage_score
                                                           │
                                   stg_benefits ───────────┴── fct_plan_benefits
```

**Materialization by layer:**

| Folder      | Materialization | Purpose                                      |
|-------------|-----------------|----------------------------------------------|
| `staging/`  | view            | Clean and deduplicate raw source tables      |
| `marts/`    | table           | Conformed dimensions ready for reuse         |
| `facts/`    | table           | Business metrics and scored outputs          |

---

## Staging Layer (`models/staging/`)

Staging models are thin views — one per source table. They standardize column names, cast types, and absorb duplicate rows caused by extraction bugs in the current data snapshot. No joins occur at this layer.

### `stg_plans`

**Grain:** one row per plan  
**Source:** `raw_data.plans`

Renames `id → plan_id`, `type → plan_type`. Converts boolean columns from string. Flattens `disease_mgmt_programs` from a DuckDB `VARCHAR[]` array to a comma-separated string for BI compatibility. Passes through 28 source columns, selecting the 13 most analytically relevant.

### `stg_benefits`

**Grain:** one row per plan × benefit_type × network_tier  
**Source:** `raw_data.benefits`

Casts `covered → BOOLEAN`, `copay → DECIMAL(10,2)`, `coinsurance_rate → DECIMAL(5,4)`. Adds `is_mental_health_benefit` flag for the two MH benefit types (`MENTAL_BEHAVIORAL_HEALTH_INPATIENT_SERVICES`, `MENTAL_BEHAVIORAL_HEALTH_OUTPATIENT_SERVICES`).

Uses `SELECT DISTINCT` to absorb ~7.8× duplicate rows from the page-accumulation extraction bug. After the next clean extraction run, this is a no-op.

### `stg_deductibles`

**Grain:** one row per plan × deductible_type × network_tier × family_cost  
**Source:** `raw_data.deductibles`

Renames `type → deductible_type`. Casts `amount → DECIMAL(10,2)`. Drops `display_string` (always empty).

### `stg_moops`

**Grain:** one row per plan × moop_type × network_tier × CSR tier × family_cost  
**Source:** `raw_data.moops`

Renames `type → moop_type`. Casts `amount → DECIMAL(10,2)`, `individual`/`family → BOOLEAN`.

### `stg_issuer`

**Grain:** one row per plan_id × issuer_id  
**Source:** `raw_data.issuer`

Groups by `(plan_id, issuer_id)` — `MIN()` aggregation absorbs duplicate rows from the extraction bug. Filters out rows with null `issuer_id`. Does not yet deduplicate to issuer grain (that is `dim_issuer`'s job).

### `stg_rating`

**Grain:** one row per plan  
**Source:** `raw_data.rating`

Groups by `plan_id` — `MAX()` aggregation absorbs duplicates. Converts 0-star ratings to `NULL` using `NULLIF(..., 0)`, because `0` on the CMS Quality Rating System (QRS) means "not yet rated", not an actual score. The `global_not_rated_reason` column preserves the explanation. Uses `BOOL_OR(available)` to aggregate the boolean availability flag.

---

## Marts Layer (`models/marts/`)

Dimension tables. Each joins two or more staging models to produce a conformed, reusable entity. Materialized as tables.

### `dim_issuer`

**Grain:** one row per insurance carrier (issuer_id)  
**Source:** `stg_issuer`

Aggregates from the plan-level `stg_issuer` to carrier level using `MIN()` and `COUNT(*)`. Produces `plan_count` — the number of plans this carrier offers in the current market snapshot.

**8 carriers** in the Marion County, FL 2025 individual market.

### `dim_plan`

**Grain:** one row per plan  
**Sources:** `stg_plans` LEFT JOIN `stg_issuer` LEFT JOIN `stg_rating`

The central dimension. Joins plan attributes, carrier name, and all five CMS quality rating dimensions onto a single row per plan. This is the primary join point for any query that needs plan attributes alongside metrics — downstream fact models join here instead of repeating the same joins.

**Note:** 9 plans with `$0` deductibles are missing issuer and rating rows in the current data snapshot due to a pre-fix extraction bug. They appear in `dim_plan` with `NULL` carrier and rating fields via LEFT JOIN. This resolves on the next clean run.

---

## Facts Layer (`models/facts/`)

Aggregated and scored outputs. Materialized as tables.

### `fct_plan_benefits`

**Grain:** one row per plan × benefit_type × network_tier  
**Sources:** `stg_benefits`  
**Row count:** ~3,146 (159 plans × ~20 benefit types × ~1–3 network tiers)

A flat benefit fact table. Preserves all cost-sharing detail — copay, coinsurance rate, covered flag, MH flag — at the finest available grain. The surrogate key `benefit_key` is `md5(plan_id || '|' || benefit_type || '|' || network_tier)`.

Use this table for benefit-level comparisons: which plans cover a given service, at what copay, across network tiers.

**Key columns:**

| Column                 | Description                                                  |
|------------------------|--------------------------------------------------------------|
| `benefit_key`          | Surrogate PK (md5)                                           |
| `plan_id`              | FK → `dim_plan`                                              |
| `benefit_type`         | Standardized CMS benefit category code                       |
| `benefit_name`         | Human-readable name                                          |
| `network_tier`         | `In-Network`, `Out-of-Network`, or `In-Network Tier 2`       |
| `covered`              | Whether the benefit is covered                               |
| `copay`                | Fixed dollar copay for this tier                             |
| `coinsurance_rate`     | Percentage of cost after deductible (0.0–1.0)                |
| `is_mental_health_benefit` | True for inpatient and outpatient MH benefit types       |

---

### `fct_plan_mh_coverage_score`

**Grain:** one row per plan (159 rows)  
**Sources:** `dim_plan`, `stg_benefits`, `stg_deductibles`, `stg_moops`

The primary analytical output. Each plan is scored 0–100 on four dimensions of mental health coverage quality. Use this table to rank plans, compare carriers, or filter by metal tier.

**Key columns:**

| Column                   | Description                                                                |
|--------------------------|----------------------------------------------------------------------------|
| `plan_id`                | PK — unique plan identifier                                                |
| `carrier`                | Carrier name; falls back to CMS issuer ID prefix if missing                |
| `metal_tier`             | Catastrophic / Bronze / Silver / Gold / Platinum                           |
| `plan_type`              | HMO / PPO / EPO / POS                                                      |
| `premium`                | Monthly premium in dollars                                                 |
| `mh_benefits_covered`    | Count of MH benefit types that are covered (max 2)                         |
| `avg_mh_copay`           | Average in-network outpatient MH copay across cost-sharing tiers           |
| `in_network_deductible`  | Combined in-network EHB deductible (individual); defaults to $9,200        |
| `in_network_moop`        | In-network max out-of-pocket (individual); defaults to $9,200              |
| `global_rating`          | CMS QRS global rating (1–5 stars); 0 = not yet rated                       |
| `coverage_score`         | Composite score 0–100 (see formula below)                                  |

#### Coverage Score Formula

```
coverage_score =
    (mh_benefits_covered / 2)  × 20   -- MH coverage completeness (0–20 pts)
  + (1 − avg_mh_copay / 125)   × 40   -- Outpatient copay efficiency (0–40 pts)
  + (1 − deductible / 9200)    × 25   -- Deductible access (0–25 pts)
  + (global_rating / 5)        × 15   -- CMS quality rating (0–15 pts)
```

**Calibration against this market (Marion County, FL 2025):**

| Dimension          | Market range  | Rationale                                       |
|--------------------|---------------|-------------------------------------------------|
| MH copay           | $0 – $125     | Max observed outpatient in-network copay         |
| Deductible         | $0 – $9,200   | ACA individual market statutory maximum          |
| CMS rating         | 0 – 5 stars   | CMS QRS scale                                   |

**Score distribution (current snapshot):**

| Statistic | Score |
|-----------|-------|
| Maximum   | 93.8  |
| Median    | 64.6  |
| Mean      | 59.6  |
| Minimum   | 0.0   |

Top scorers are Platinum HMO/EPO plans with $0 deductibles, $10 outpatient copays, and 4-star CMS ratings (AvMed, Florida Blue). Bottom scorers are Catastrophic plans — excluded from ratings and with high cost-sharing.

---

## Test Coverage

Tests are defined in `staging/staging.yml`, `marts/marts.yml`, and `facts/schema.yml`.

| Test type                    | Applied to                                                       |
|------------------------------|------------------------------------------------------------------|
| `unique` + `not_null`        | All primary keys across every model                              |
| `not_null`                   | All non-nullable dimension and fact columns                      |
| `relationships`              | `fct_plan_benefits.plan_id` → `dim_plan`, `fct_plan_mh_coverage_score.plan_id` → `dim_plan`, `dim_plan.issuer_id` → `dim_issuer` |

Run all tests:

```bash
dbt test --target dev
```

---

## Adding dbt Packages

To add a package (e.g., `dbt_utils`), edit `packages.yml`:

```yaml
packages:
  - package: dbt-labs/dbt_utils
    version: ">=1.3.0"
```

Then run `dbt deps` — it installs packages into `dbt_packages/` (gitignored). The pipeline's `run_dbt()` function calls `dbt deps` automatically before every run.
