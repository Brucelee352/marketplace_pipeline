"""
#----------------------------------------------------------------#

Marketplace Data Pipeline

An ELT pipeline built to evaluate mental health services as a
benefit across insurance plans offered on the US Public Health
Insurance marketplace across selected Florida counties that I 
have previously resided in.

Please refer to README.md file for more information.

#----------------------------------------------------------------#
"""

import requests as req
import duckdb as db
import pandas as pd
import sys
import os
import logging
from dbt.cli.main import dbtRunner
from contextlib import contextmanager
from dotenv import load_dotenv
from datetime import datetime
from pathlib import Path


# Setup
root = Path(__file__).parents[1]
load_dotenv(root / ".env")
base_url = os.getenv("BASE_URL")
key = os.getenv("API_KEY")
log = logging.getLogger(__name__)
dbt_root = root / "marketplace_pipeline"
DB_PATH = Path(__file__).parent.parent / "marketplace.duckdb"

COUNTIES = {
    "12083": {"name": "Marion",       "zip": "34470"},
    "12057": {"name": "Hillsborough", "zip": "33602"},
    "12011": {"name": "Broward",      "zip": "33301"},
    "12095": {"name": "Orange",       "zip": "32801"},
}

# Configure API session
session = req.Session()
session.params = {"apikey": key}


def setup_logging() -> logging.Logger:
    log_dir = Path(root) / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_dir / "pipeline.log"),
            logging.StreamHandler(sys.stdout),
        ],
        force=True, 
    )
    return logging.getLogger(__name__)
log = setup_logging()

# Extract


def _fetch_county(
    fips: str,
    county_name: str,
    zip_code: str,
    state: str = "FL",
    year: int = 2025,
) -> dict[str, pd.DataFrame]:
    """Paginate the Marketplace API for a single county and return six DataFrames."""

    all_plans: list = []
    benefits: list = []
    deds: list = []
    moops: list = []
    ratings: list = []
    issuers: list = []
    total = 0

    limit = 25
    offset = 0

    payload = {
        "household": {
            "people": [
                {
                    "is_pregnant": False,
                    "is_parent": False,
                    "uses_tobacco": False,
                    "gender": "Male",
                }
            ],
            "has_married_couple": False,
        },
        "place": {"countyfips": fips, "state": state, "zipcode": zip_code},
        "market": "Individual",
        "sort": "premium",
        "order": "asc",
        "year": year,
    }

    while True:
        try:
            payload["limit"] = limit
            payload["offset"] = offset

            res = session.post(f"{base_url}/plans/search", json=payload)
            res.raise_for_status()

            data = res.json()
            if "error" in data:
                log.error("[%s] API error: %s", county_name, data)
                break

            plans = data.get("plans", [])
            total = data.get("total", 0)
            all_plans.extend(plans)

            log.info(
                "[%s] Page %d: got %d plans (%d/%d)",
                county_name,
                offset // limit + 1,
                len(plans),
                len(all_plans),
                total,
            )

            offset += len(plans)
            if not plans or offset >= total:
                break
        except (req.RequestException, ValueError, KeyError) as e:
            log.error("[%s] Pagination error — terminating: %s", county_name, e)
            break

    for plan in all_plans:
        for benefit in plan.get("benefits", []):
            for cs in benefit.get("cost_sharings", []):
                benefits.append(
                    {
                        "plan_id": plan["id"],
                        "plan_name": plan["name"],
                        "premium": plan["premium"],
                        "benefit_type": benefit["type"],
                        "benefit_name": benefit["name"],
                        "covered": benefit["covered"],
                        "network_tier": cs["network_tier"],
                        "copay": cs["copay_amount"],
                        "coinsurance_rate": cs["coinsurance_rate"],
                    }
                )

        for ded in plan.get("deductibles", []):
            deds.append(
                {
                    "plan_id": plan["id"],
                    "network_tier": ded.get("network_tier"),
                    "type": ded.get("type"),
                    "amount": ded.get("amount"),
                    "family_cost": ded.get("family_cost"),
                    "display_string": ded.get("display_string", ""),
                }
            )

        for moop in plan.get("moops", []):
            moops.append(
                {
                    "plan_id": plan["id"],
                    "network_tier": moop.get("network_tier"),
                    "type": moop.get("type"),
                    "amount": moop.get("amount"),
                    "csr": moop.get("csr"),
                    "family_cost": moop.get("family_cost"),
                    "individual": moop.get("individual"),
                    "family": moop.get("family"),
                }
            )

        rating = plan.get("quality_rating") or {}
        ratings.append(
            {
                "plan_id": plan["id"],
                "available": rating.get("available"),
                "global_rating": rating.get("global_rating"),
                "clinical_quality_mgmt_rating": rating.get(
                    "clinical_quality_management_rating"
                ),
                "enrollee_experience_rating": rating.get("enrollee_experience_rating"),
                "plan_efficiency_rating": rating.get("plan_efficiency_rating"),
                "global_not_rated_reason": rating.get("global_not_rated_reason"),
                "enrollee_experience_not_rated_reason": rating.get(
                    "enrollee_experience_not_rated_reason"
                ),
                "plan_efficiency_not_rated_reason": rating.get(
                    "plan_efficiency_not_rated_reason"
                ),
            }
        )

        issuer = plan.get("issuer") or {}
        issuers.append(
            {
                "plan_id": plan["id"],
                "issuer_id": issuer.get("id"),
                "name": issuer.get("name"),
                "state": issuer.get("state"),
                "individual_url": issuer.get("individual_url"),
                "shop_url": issuer.get("shop_url"),
                "toll_free": issuer.get("toll_free"),
                "tty_number": issuer.get("tty"),
            }
        )

    log.info("[%s] Collected %d of %d plans", county_name, len(all_plans), total)

    return {
        "plans": pd.DataFrame(all_plans),
        "benefits": pd.DataFrame(benefits),
        "deductibles": pd.DataFrame(deds),
        "moops": pd.DataFrame(moops),
        "issuer": pd.DataFrame(issuers),
        "rating": pd.DataFrame(ratings),
    }


def runapi(
    counties: dict[str, dict] = COUNTIES,
    state: str = "FL",
    year: int = 2025,
) -> dict[str, dict[str, pd.DataFrame]]:
    """
    Query the Marketplace API for each county.
    Returns {fips: {table_name: DataFrame}}.
    Each DataFrame includes county_fips and county_name as the first two columns.
    """
    results: dict[str, dict[str, pd.DataFrame]] = {}

    for fips, info in counties.items():
        dfs = _fetch_county(fips, info["name"], info["zip"], state, year)

        for name, df in dfs.items():
            if df.empty and len(df.columns) == 0:
                log.warning("[%s] %s is fully empty — skipping county tagging", info["name"], name)
                continue
            df.insert(0, "county_name", info["name"])
            df.insert(0, "county_fips", fips)
           
        results[fips] = dfs
        log.info("Completed county: %s (%s)", info["name"], fips)

    return results


# Load


def create_tables(df: pd.DataFrame, table_name: str, con: db.DuckDBPyConnection) -> None:
    """Create or replace a source table in DuckDB from a DataFrame."""
    log.info("Loading %s: shape=%s", table_name, df.shape)
    if df.empty and len(df.columns) == 0:
        log.warning("Skipping %s — empty dataframe with no columns", table_name)
        return

    con.execute(
        f"CREATE OR REPLACE TABLE raw_data.{table_name} AS SELECT * FROM df"
    )

    exists = con.execute(
        "SELECT 1 FROM information_schema.tables WHERE table_schema='raw_data' AND table_name=?",
        [table_name],
    ).fetchone()
    if not exists:
        raise RuntimeError(f"raw_data.{table_name} was not created (df shape: {df.shape})")

    row = con.execute(f"SELECT COUNT(*) FROM raw_data.{table_name}").fetchone()
    count = row[0] if row else 0
    log.info("raw_data.%s: %d rows", table_name, count)


# Transform


def _invoke(dbt: dbtRunner, args: list[str], dbt_root: Path) -> None:
    """Run a dbt command and raise on failure."""
    full_args = args + ["--project-dir", str(dbt_root)]
    result = dbt.invoke(full_args)
    if not result.success:
        if result.exception:
            raise RuntimeError(f"dbt {' '.join(args)} failed: {result.exception}") from result.exception
        raise RuntimeError(f"dbt {' '.join(args)} failed (check dbt logs)")

def run_dbt(*, full_refresh: bool = False) -> None:
    """Run dbt build for the dev target."""
    log.info("Running dbt against %s", dbt_root)
    dbt = dbtRunner()

    _invoke(dbt, ["deps"], dbt_root)

    build_args = ["build", "--target", "dev"]
    if full_refresh:
        build_args.append("--full-refresh")
    _invoke(dbt, build_args, dbt_root)

    log.info("dbt build completed successfully")


if __name__ == "__main__":

    log.info("Running Pipeline...")

    ts = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    snapshot_dir = Path(root) / "data" / "snapshots"
    snapshot_dir.mkdir(parents=True, exist_ok=True)


    # Fetch all counties — {fips: {table: df}}
    log.info("Sourcing data from API and compiling dataframes.")
    county_results = runapi(COUNTIES)
    

    # Per-county timestamped CSV snapshots
    for fips, dfs in county_results.items():
        county_name = COUNTIES[fips]["name"]
        for table_name, df in dfs.items():
            df.to_csv(
                snapshot_dir / f"{county_name}_{table_name}_{ts}.csv", index=False
            )

    # Combine all counties into one DataFrame per table
    frames: dict[str, list[pd.DataFrame]] = {}
    for dfs in county_results.values():
        for table_name, df in dfs.items():
            frames.setdefault(table_name, []).append(df)

    combined = {
        name: pd.concat(dfs, ignore_index=True) for name, dfs in frames.items()
    }

    # Latest combined CSVs (overwritten each run)
    for table_name, df in combined.items():
        df.to_csv(Path(root) / "data" / f"{table_name}.csv", index=False)

    # Load into DuckDB
    log.info("Loading data into DuckDB...")
    con = db.connect(str(DB_PATH), read_only=True)
    con.execute("CREATE SCHEMA IF NOT EXISTS raw_data")
    try:
        for table_name, df in combined.items():
            create_tables(df, table_name, con)
    finally:
        con.close()

    # Transform
    log.info("Transforming data using dbt...")
    run_dbt()
