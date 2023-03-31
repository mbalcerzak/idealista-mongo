"""
Script to create JSON in a format read by mabdata.com charts

{
"dates": {
    "max_date": "",
    "min_date": ""
    },
"flats_per_location": {},
"flats_per_area_cat": {},
"scraped_per_day": {},
"scraped_per_day_m_avg": {},
"changes_per_day": {},
"changed_per_day_m_avg": {},
"scraped_per_month": {},
"posted_per_day": {},
"posted_per_day_m_avg": {},
"price_m_location": [],
"price_m_loc_area_cat": []
}
"""
import json
import pandas as pd
from datetime import date
from dateutil.relativedelta import relativedelta

from flat_info import save_json
from utils import get_price_records_data
from db_mongo import get_db


def get_dates() -> dict:
    """Get max and min dates from the database"""
    db = get_db()
    collection_flats = db["_flats"]

    max_cursor = collection_flats.find().sort('date',-1).limit(1)
    max_date = [c['date'] for c in max_cursor][0]

    min_cursos = collection_flats.find().sort('date',1).limit(1)
    min_date = [c['date'] for c in min_cursos][0]

    return {"max_date":max_date, "min_date":min_date}


def get_flats_per_location() -> dict:
    """Get flats per district"""

def get_flats_per_area_cat() -> dict:
    """Get flats per size of the flat (every 10m2)"""

def get_scraped_per_day() -> dict:
    """Get number of flats scraped per day"""

def get_scraped_per_day_m_avg() -> dict:
    """Get moving average of number of flats scraped per day"""

def get_changes_per_day() -> dict:
    """Get flats price changes per day"""

def get_schanged_per_day_m_avg() -> dict:
    """Get moving average of number of flats price changes per day"""

def get_scraped_per_month() -> dict:
    """Get number of flats scraped per day"""

def get_posted_per_day() -> dict:
    """Get number of flats posted per day"""

def get_posted_per_day_m_avg() -> dict:
    """Get moving average of number of flats posted per day"""

def get_price_m_location() -> dict:
    """Get price per location per month"""

def get_price_m_loc_area_cat() -> dict:
    """Get price per location per month per flat size category"""


if __name__ == "__main__":
    dates = get_dates()