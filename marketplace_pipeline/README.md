# marketplace_pipeline — dbt Project

This dbt project transforms raw Marketplace API data into a clean dimensional model and a scored mental health coverage fact table. It connects to `marketplace.duckdb` (written by `scripts/main.py`) and builds all models in the same database.

---

## Connection

Profile: `marketplace_pipeline` (configured in `~/.dbt/profiles.yml`)
Target: `dev` -> `../marketplace.duckdb` (relative to this directory)
Adapter: `dbt-duckdb`

```bash
dbt snapshot --target dev   # run first, captures plan history
dbt run --target dev
dbt test --target dev
```

---

## Model DAG

```
raw_data (DuckDB schema, written by main.py)
|
+-- snp_plans  (snapshots schema, SCD Type 2, check strategy)
|
+-- stg_plans ───────────────────────────────┐
+-- stg_benefits ──────────────────────┐     │
+-- stg_deductibles ───────────────┐   │     ├── dim_plan ──┐
+-- stg_moops ─────────────────┐   │   │     │              │
+-- stg_issuer ────────────────┼───┼───┘     +-- dim_issuer │
+-- stg_rating ────────────────┘   │                        │
                                   │                        │
                                   └────────────────────────┼── fct_plan_mh_coverage_score
                                                            │
                                   stg_benefits ────────────┴── fct_plan_benefits
```

**Materialization by layer:**

| Folder     | Materialization | Purpose                                      |
|------------|-----------------|----------------------------------------------|
| `staging/` | view            | Clean and deduplicate raw source tables      |
| `marts/`   | table           | Conformed dimensions ready for reuse         |
| `facts/`   | table           | Business metrics and scored outputs          |

---

## Snapshots (`snapshots/`)

### `snp_plans`

**Strategy:** `check`
**Unique key:** `id` (plan ID)
**Schema:** `snapshots` (separate from `main`)
**Tracked columns:** `premium`, `metal_level`, `type`, `design_type`, `hsa_eligible`, `is_standardized_plan`

Implements SCD Type 2 on the `plans` source without requiring a source-provided date column. The source table contains one row per county x plan, so the snapshot query deduplicates with `any_value() GROUP BY id` before snapshotting. Plan design attributes are identical across counties for the same `plan_id`, so `any_value()` is safe.

On each pipeline run, dbt compares the tracked columns against the current snapshot state. When any tracked column changes, dbt:

1. Closes the old row by setting `dbt_valid_to` to the current timestamp
2. Inserts a new row with `dbt_valid_from` set to the same timestamp and `dbt_valid_to = NULL`

dbt-managed columns added to the snapshot table:

| Column           | Meaning                                                    |
|------------------|------------------------------------------------------------|
| `dbt_scd_id`     | Surrogate key for the historical record                    |
| `dbt_valid_from` | When dbt first observed or detected this version           |
| `dbt_valid_to`   | When this version was superseded (`NULL` = current record) |
| `dbt_updated_at` | System timestamp used as the change boundary               |

**Query current plans:**
```sql
select * from snapshots.snp_plans where dbt_valid_to is null;
```

**Query plan history:**
```sql
select * from snapshots.snp_plans where id = '49004FL0010001' order by dbt_valid_from;
```

The `check` strategy is appropriate here because the CMS Marketplace API does not expose an `updated_at` field, so changes are detected purely from column value comparison across runs.

---

## Staging Layer (`models/staging/`)

Staging models are thin views, one per source table. They standardize column names, cast types, and add county context. No joins occur at this layer.

All staging models include `county_fips` and `county_name` as the first two columns, propagated from the raw source tables.

### `stg_plans`

**Grain:** one row per county x plan
**Source:** `raw_data.plans`

Adds `plan_key = md5(county_fips || '|' || plan_id)` as a composite surrogate primary key, necessary because the same `plan_id` can appear in multiple counties. Renames `id -> plan_id`, `type -> plan_type`. Converts boolean columns from string. Flattens `disease_mgmt_programs` from a DuckDB `VARCHAR[]` array to a comma-separated string for BI compatibility. Passes through 17 source columns.

### `stg_benefits`

**Grain:** one row per county x plan x benefit_type x network_tier
**Source:** `raw_data.benefits`

Casts `covered -> BOOLEAN`, `copay -> DECIMAL(10,2)`, `coinsurance_rate -> DECIMAL(5,4)`. Adds `is_mental_health_benefit` flag for the two MH benefit types (`MENTAL_BEHAVIORAL_HEALTH_INPATIENT_SERVICES`, `MENTAL_BEHAVIORAL_HEALTH_OUTPATIENT_SERVICES`). Uses `SELECT DISTINCT` to absorb duplicate rows in the current snapshot.

### `stg_deductibles`

**Grain:** one row per county x plan x deductible_type x network_tier x family_cost
**Source:** `raw_data.deductibles`

Renames `type -> deductible_type`. Casts `amount -> DECIMAL(10,2)`. Drops `display_string` (always empty in the API response).

### `stg_moops`

**Grain:** one row per county x plan x moop_type x network_tier x CSR tier x family_cost
**Source:** `raw_data.moops`

Renames `type -> moop_type`. Casts `amount -> DECIMAL(10,2)`, `individual`/`family -> BOOLEAN`. The `csr` column is non-null only for Silver plans, which carry CSR-73, CSR-87, and CSR-94 variants for income-eligible enrollees.

### `stg_issuer`

**Grain:** one row per plan_id x issuer_id (not county-level)
**Source:** `raw_data.issuer`

Groups by `(plan_id, issuer_id)`, with `MIN()` aggregation to absorb duplicate rows and collapse multi-county data. Issuer attributes are plan-level metadata and do not vary by county. Filters out rows with null `issuer_id`.

### `stg_rating`

**Grain:** one row per plan_id (not county-level)
**Source:** `raw_data.rating`

Groups by `plan_id`, with `MAX()` aggregation to absorb duplicates. CMS quality ratings are national plan-level scores and do not vary by county. Converts 0-star ratings to `NULL` using `NULLIF(..., 0)` because `0` on the CMS Quality Rating System (QRS) means "not yet rated", not an actual score. The `global_not_rated_reason` column preserves the explanation. Uses `BOOL_OR(available)` to aggregate the boolean availability flag.

---

## Marts Layer (`models/marts/`)

Dimension tables. Each joins two or more staging models to produce a conformed, reusable entity. Materialized as tables.

### `dim_issuer`

**Grain:** one row per insurance carrier (`issuer_id` is the PK)
**Source:** `stg_issuer`

Aggregates from the plan-level `stg_issuer` to carrier level using `MIN()` and `COUNT(DISTINCT plan_id)`. Produces `plan_count`, the number of distinct plans this carrier offers across the four-county market snapshot.

### `dim_plan`

**Grain:** one row per county x plan (`plan_key` is the surrogate PK)
**Sources:** `stg_plans` LEFT JOIN `stg_issuer` LEFT JOIN `stg_rating`

The central dimension. Joins plan attributes (county-level) with carrier name and all five CMS quality rating dimensions (plan-level). The same `plan_id` appears in up to four rows, one per county, each with its own `plan_key`. Carrier name, issuer ID, and CMS quality ratings are identical across these rows as they are national plan-level attributes.

The LEFT JOIN on issuer and rating is intentional: 9 plans with `$0` deductibles are missing issuer and rating rows in the current snapshot. They appear in `dim_plan` with `NULL` carrier and rating fields and resolve on the next clean pipeline run.

---

## Facts Layer (`models/facts/`)

Aggregated and scored outputs. Materialized as tables.

### `fct_plan_benefits`

**Grain:** one row per county x plan x benefit_type x network_tier
**Sources:** `stg_benefits`, `stg_plans`

A flat benefit fact table. Preserves all cost-sharing detail (copay, coinsurance rate, covered flag, MH flag) at the finest available grain. The surrogate key `benefit_key` is `md5(county_fips || '|' || plan_id || '|' || benefit_type || '|' || network_tier)`.

Use this table for benefit-level comparisons: which plans cover a given service, at what copay, across network tiers and counties.

**Key columns:**

| Column                     | Description                                                  |
|----------------------------|--------------------------------------------------------------|
| `benefit_key`              | Surrogate PK (md5 of county x plan x benefit x tier)         |
| `county_fips`              | Five-digit FIPS code                                         |
| `county_name`              | Human-readable county name                                   |
| `plan_id`                  | FK -> `dim_plan` (join on county_fips + plan_id)             |
| `benefit_type`             | Standardized CMS benefit category code                       |
| `benefit_name`             | Human-readable name                                          |
| `network_tier`             | `In-Network`, `Out-of-Network`, or `In-Network Tier 2`       |
| `covered`                  | Whether the benefit is covered                               |
| `copay`                    | Fixed dollar copay for this tier                             |
| `coinsurance_rate`         | Percentage of cost after deductible (0.0-1.0)                |
| `is_mental_health_benefit` | True for inpatient and outpatient MH benefit types           |

---

### `fct_plan_mh_coverage_score`

**Grain:** one row per county x plan (`plan_key` is the PK)
**Sources:** `dim_plan`, `stg_benefits`, `stg_deductibles`, `stg_moops`

The primary analytical output. Each county x plan combination is scored 0-100 on four dimensions of mental health coverage quality. Use this table to rank plans, compare carriers, or filter by county and metal tier.

**Key columns:**

| Column                  | Description                                                                |
|-------------------------|----------------------------------------------------------------------------|
| `plan_key`              | Surrogate PK, md5(county_fips + plan_id), FK -> `dim_plan.plan_key`        |
| `county_fips`           | Five-digit FIPS code                                                       |
| `county_name`           | Human-readable county name                                                 |
| `plan_id`               | CMS plan identifier, not unique alone across counties                      |
| `carrier`               | Carrier name, falls back to CMS issuer ID prefix if missing                |
| `metal_tier`            | Catastrophic / Bronze / Silver / Gold / Platinum                           |
| `plan_type`             | HMO / PPO / EPO / POS                                                      |
| `premium`               | Monthly premium in dollars                                                 |
| `mh_benefits_covered`   | Count of MH benefit types that are covered (max 2)                         |
| `avg_mh_copay`          | Average in-network outpatient MH copay across cost-sharing tiers           |
| `in_network_deductible` | Combined in-network EHB deductible (individual), defaults to $9,200        |
| `in_network_moop`       | In-network max out-of-pocket (individual), defaults to $9,200              |
| `global_rating`         | CMS QRS global rating (1-5 stars), NULL = not yet rated                    |
| `coverage_score`        | Composite score 0-100 (see formula below)                                  |

#### Coverage Score Formula

```
coverage_score =
    (mh_benefits_covered / 2)  x 20   -- MH coverage completeness (0-20 pts)
  + (1 - avg_mh_copay / 125)   x 40   -- Outpatient copay efficiency (0-40 pts)
  + (1 - deductible / 9200)    x 25   -- Deductible access (0-25 pts)
  + (global_rating / 5)        x 15   -- CMS quality rating (0-15 pts)
```

**Calibration against this market (FL 2025 individual market):**

| Dimension  | Market range | Rationale                               |
|------------|--------------|-----------------------------------------|
| MH copay   | $0 - $125    | Max observed outpatient in-network copay |
| Deductible | $0 - $9,200  | ACA individual market statutory maximum  |
| CMS rating | 0 - 5 stars  | CMS QRS scale                            |

Top scorers are Platinum HMO/EPO plans with $0 deductibles, $10 outpatient copays, and 4-star CMS ratings. Bottom scorers are Catastrophic plans, which are excluded from CMS ratings and carry high cost-sharing.

---

## Test Coverage

Tests are defined in `staging/staging.yml`, `marts/marts.yml`, and `facts/schema.yml`.

| Test type             | Applied to                                                                        |
|-----------------------|-----------------------------------------------------------------------------------|
| `unique` + `not_null` | All primary keys across every model                                               |
| `not_null`            | All non-nullable dimension and fact columns                                       |
| `relationships`       | `dim_plan.issuer_id` -> `dim_issuer`, `fct_plan_mh_coverage_score.plan_key` -> `dim_plan.plan_key` |

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

Then run `dbt deps`, it installs packages into `dbt_packages/` (gitignored). The pipeline's `run_dbt()` function calls `dbt deps` automatically before every run.