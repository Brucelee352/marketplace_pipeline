"""
#----------------------------------------------------------------#

Marketplace Data Pipeline v1.0

An ETL pipeline built to evaluate mental health services as a 
benefit across insurance plans offered on the US Public Health 
Insurance marketplace in Marion County, FL. 

Please refer to README.md file for more information.

#----------------------------------------------------------------#
"""

# Standard library imports

import requests as req
import duckdb as db
import pandas as pd
import pandera as pa
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
plan_id = os.getenv("PLAN_ID")
design_types = os.getenv("STANDARD_DESIGN_TYPES")
mental_health_types = os.getenv("MENTAL_HEALTH_TYPES")
log = logging.getLogger(__name__)
dbt_root = root / 'marketplace_pipeline'

## Configure API 
session = req.Session()
session.params = {"apikey": key}

### Configure Logging

def setup_logging() -> logging.Logger:
    log_dir = Path(root) / 'logs'
    log_dir.mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        level=os.getenv('LOG_LEVEL', 'INFO'),
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_dir / 'pipeline.log'),
            logging.StreamHandler(sys.stdout),
        ],
    )
    return logging.getLogger(__name__)


# Extract 
def runapi(fips, state, zip, year=2025) -> dict[str, pd.DataFrame]:
    """
    This function queries the Markplace API via pagination, and drops unneeded columns.
    """

    all_plans = []
    benefits = []
    deds = []
    moops = []
    ratings = []
    issuers = []

    # include data snapshots for logging purposes
    snapshot_dir = Path(root) / "data" / "snapshots"
    data_dir = Path(root) / "data"
    snapshot_dir.mkdir(parents=True, exist_ok=True)

    ts = datetime.now().strftime("%Y-%m-%d_%H%M%S")  # e.g. "2026-05-01_143022"

    limit = 25
    offset = 0

    payload = {
        "household": {
        
        "people": [
            {
                "is_pregnant": False,
                "is_parent": False,
                "uses_tobacco": False,
                "gender": "Male"
            }
        ],
        "has_married_couple": False
            },
        "place": {
            "countyfips": fips,
            "state": state,
            "zipcode": zip
        },
        "market": "Individual",
        "sort": "premium",
        "order": "asc",
        "year": year
    }
    while True:
        try:
            payload["limit"] = limit
            payload["offset"] = offset

            res = session.post(              
                f"{base_url}/plans/search", json=payload)
            res.raise_for_status()   
            
            data = res.json()
            if "error" in data:
                log.error(f"API error: {data}")
                break

            plans = data.get("plans", [])
            total = data.get("total", 0)
            all_plans.extend(plans)

            log.info(f"Page {offset // limit + 1}: got {len(plans)} rows out of: ({len(all_plans)}/{total})")

            offset += len(plans)
            if not plans or offset >= total:
                break
        except KeyError as e:
            log.error("Key not found! Terminating loop.")
            break

    for plan in all_plans:
        for benefit in plan.get("benefits", []):
            for cs in benefit.get("cost_sharings", []):
                benefits.append({
                    "plan_id": plan["id"],
                    "plan_name": plan["name"],
                    "premium": plan["premium"],
                    "benefit_type": benefit["type"],
                    "benefit_name": benefit["name"],
                    "covered": benefit["covered"],
                    "network_tier": cs["network_tier"],
                    "copay": cs["copay_amount"],
                    "coinsurance_rate": cs["coinsurance_rate"],
                })
    
        for ded in plan.get("deductibles", []):
            deds.append({
                "plan_id": plan["id"],
                "network_tier": ded.get("network_tier"),
                "type": ded.get("type"),
                "amount": ded.get("amount"),
                "family_cost": ded.get("family_cost"),
                "display_string": ded.get("display_string", ""),
            })

        for moop in plan.get("moops", []):
            moops.append({
                "plan_id": plan["id"],
                "network_tier": moop.get("network_tier"),
                "type": moop.get("type"),
                "amount": moop.get("amount"),
                "csr": moop.get("csr"),
                "family_cost": moop.get("family_cost"),
                "individual": moop.get("individual"),
                "family": moop.get("family"),
            })

        rating = plan.get("quality_rating") or {}
        ratings.append({
            "plan_id": plan["id"],
            "available": rating.get("available"),
            "global_rating": rating.get("global_rating"),
            "clinical_quality_mgmt_rating": rating.get("clinical_quality_management_rating"),
            "enrollee_experience_rating": rating.get("enrollee_experience_rating"),
            "plan_efficiency_rating": rating.get("plan_efficiency_rating"),
            "global_not_rated_reason": rating.get("global_not_rated_reason"),
            "enrollee_experience_not_rated_reason": rating.get("enrollee_experience_not_rated_reason"),
            "plan_efficiency_not_rated_reason": rating.get("plan_efficiency_not_rated_reason"),
        })

        issuer = plan.get("issuer") or {}
        issuers.append({
            "plan_id": plan["id"],
            "issuer_id": issuer.get("id"),
            "name": issuer.get("name"),
            "state": issuer.get("state"),
            "individual_url": issuer.get("individual_url"),
            "shop_url": issuer.get("shop_url"),
            "toll_free": issuer.get("toll_free"),
            "tty_number": issuer.get("tty"),
        })



    # ---- Sub dataframes
    benefits_df = pd.DataFrame(benefits)
    deductibles_df = pd.DataFrame(deds)
    moops_df = pd.DataFrame(moops)
    rating_df = pd.DataFrame(ratings)
    issuer_df = pd.DataFrame(issuers)

    log.info(f"Collected {len(all_plans)} of {total} plans")
    
    plans_df = pd.DataFrame(all_plans)

    return {
        "benefits": benefits_df,
        "deductibles": deductibles_df,
        "moops": moops_df,
        "issuer": issuer_df,
        "rating": rating_df,
        "plans": plans_df,
    }
    

def create_tables(df: pd.DataFrame, table_name: str, con: db.DuckDBPyConnection) -> None:
    """
    This function creates the source tables in DuckDB.
    """
    log.info("Loading %s: shape=%s, columns=%s", table_name, df.shape, list(df.columns))
    if df.empty and len(df.columns) == 0:
        log.warning("Skipping %s — empty dataframe with no columns", table_name)
        return
    
    
    con.register("df_to_load", df)
    con.execute(f"CREATE OR REPLACE TABLE raw_data.{table_name} AS SELECT * FROM df_to_load")

    exists = con.execute(
        "SELECT 1 FROM information_schema.tables WHERE table_schema='raw_data' AND table_name=?",
        [table_name],).fetchone()
    if not exists:
        raise RuntimeError(f"raw_data.{table_name} was not created (df shape: {df.shape})")
    
    row = con.execute(f"SELECT COUNT(*) FROM raw_data.{table_name}").fetchone()
    count = row[0] if row else 0
    log.info("raw_data.%s: %s rows", table_name, count)

@contextmanager
def working_directory(path: Path):
    """
    Temporarily change cwd; always restore, even on exception.
    """
    original = Path.cwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(original)


def _invoke(dbt: dbtRunner, args: list[str]) -> None:
    """
    Run a dbt command and raise on failure.
    """
    result = dbt.invoke(args)
    if not result.success:
        raise RuntimeError(f"dbt {' '.join(args)} failed")


def run_dbt() -> None:
    """
    Run dbt deps and build models. Steps: clean → deps → run.
    """
    
    with working_directory(dbt_root):
        log.info("Running dbt in %s", dbt_root)
        dbt = dbtRunner()

        _invoke(dbt, ["clean"])
        _invoke(dbt, ["deps"])
        _invoke(dbt, ["run", "--target", "dev", "--full-refresh"])
        _invoke(dbt, ["build"])
        log.info("dbt models built successfully")


if __name__ == "__main__":
    setup_logging() 
    
    # Set up API caller: 
    fips = os.getenv("FIPS")
    state = os.getenv("STATE")
    zip_code = os.getenv("ZIP")

    # Run the API call and return dataframes:
    dfs = runapi(fips, state, zip_code)
    
    # Create .csv artifacts and snapshots: 
    ts = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    snapshot_dir = Path(root) / "data" / "snapshots"
    snapshot_dir.mkdir(parents=True, exist_ok=True)

    for name, df in dfs.items():
        df.to_csv(snapshot_dir / f"{name}_{ts}.csv", index=False)
        df.to_csv(Path(root) / "data" / f"{name}.csv", index=False)

    # Run DuckDB connector:
    con = db.connect(f"{root}/marketplace.duckdb")
    con.execute("CREATE SCHEMA IF NOT EXISTS raw_data")
    try:
        for name, df in dfs.items():
            create_tables(df, name, con)
    finally:
        con.close()

    # Run dbt: 
    run_dbt()
