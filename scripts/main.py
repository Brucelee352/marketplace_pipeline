"""
#----------------------------------------------------------------#

Marketplace Data Pipeline v1.0

An ETL pipeline built to evaluate mental health services as a 
benefit across insurance plans offered on the US Public Health 
Insurance marketplace. 

Please refer to README.md file for more information.

#----------------------------------------------------------------#
"""

# Standard library imports

import requests as req
import duckdb as db
import dbt 
import pandas as pd
import pandera as pa
import sys
import os
import logging 
from pathlib import Path
from time import sleep

# Setup
PROJECT_ROOT = Path(__file__).parents[1]
print(PROJECT_ROOT)
BASE_URL = "https://marketplace.api.healthcare.gov/api/v1"
API_KEY = "5973JEZ7cPDVT3anL8GP73DQhprTPm2U"
PLAN_ID = "54172FL0010013"
STANDARD_DESIGN_TYPES = {"DESIGN1", "DESIGN2", "DESIGN3", "DESIGN4", "DESIGN5"}
MENTAL_HEALTH_TYPES = {
    "MENTAL_BEHAVIORAL_HEALTH_INPATIENT_SERVICES",
    "MENTAL_BEHAVIORAL_HEALTH_OUTPATIENT_SERVICES",
    "SUBSTANCE_ABUSE_DISORDER_INPATIENT_SERVICES",
    "SUBSTANCE_ABUSE_DISORDER_OUTPATIENT_SERVICES",
}


## Configure API 
session = req.Session()
session.params = {"apikey": API_KEY}

# Configure Logging
LOG = logging.getLogger(':')
LOG.setLevel(os.getenv('LOG_LEVEL', 'INFO'))
LOG_DIR = PROJECT_ROOT / 'logs'
LOG_DIR.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=LOG.level,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_DIR / 'pipeline.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
LOG_DIR.mkdir(parents=True, exist_ok=True)


# Extract 

def runapi(fips, state, zip, year=2025) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:

    all_plans = []
    df1 = []
    df2 = []
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
        payload["limit"] = limit
        payload["offset"] = offset

        res = session.post(              
            f"{BASE_URL}/plans/search", json=payload)
        res.raise_for_status()   
        
        data = res.json()
        if "error" in data:
            print(f"API error: {data}")
            break

        plans = data.get("plans", [])
        total = data.get("total", 0)
        all_plans.extend(plans)

        print(f"Page {offset // limit + 1}: got {len(plans)} ({len(all_plans)}/{total})")

        offset += len(plans)
        if offset >= total:
            break

        for plan in all_plans:
            for benefit in plan.get("benefits", []):
                for cs in benefit.get("cost_sharings", []):
                    df1.append({
                        "plan_id": plan["id"],
                        "plan_name": plan["name"],
                        "premium": plan["premium"],
                        "benefit_type": benefit["type"],
                        "benefit_name": benefit["name"],
                        "covered": benefit["covered"],
                        "network_tier": cs["network_tier"],
                        "copay": cs["copay_amount"],
                        "coinsurance_rate": cs["coinsurance_rate"],
                        "display_string": cs["display_string"],
                    })

        for plan in all_plans:
            for ded in plan.get("deductibles", []):
                df2.append({
                    "plan_id": plan["id"],
                    "network_tier": ded["network_tier"],
                    "deductible_type": ded["type"],
                    "deductible_amount": ded["amount"],
                    "family_cost": ded["family_cost"],
                    "display_string": ded.get("display_string", ""),
                })

    benefits_df = pd.DataFrame(df1)
    deductibles_df = pd.DataFrame(df2)
    

    print(f"Collected {len(all_plans)} of {total} plans")

    plans_df = pd.DataFrame(all_plans)

    # Generate .csvs 
    benefits_df.to_csv(f"{PROJECT_ROOT}/data/benefit_info.csv", index=False)
    deductibles_df.to_csv(f"{PROJECT_ROOT}/data/deductibles_df.csv", index=False)
    plans_df.to_csv(f"{PROJECT_ROOT}/data/plans_df.csv", index=False)

    return benefits_df, deductibles_df, plans_df

def data_cleaning(plans_df: pd.DataFrame,
                  benefits_df: pd.DataFrame) -> None:  
    

    plans_col_drop = []
    plans_df = plans_df.drop([col for col in plans_col_drop if col in plans_df.columns], axis=1)


# def db_con()

if __name__ == "__main__":
    runapi('12083', 'FL', '34476')

