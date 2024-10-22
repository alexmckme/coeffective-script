import os
import json
from dotenv import load_dotenv
from supabase import create_client
import gspread
import pandas as pd
import gspread_dataframe as gd
import requests
from simple_salesforce import Salesforce
from io import StringIO
from datetime import datetime, time
import time as time_two
import helpers
import pytz



# Fonctions intéragir avec SF
def sfma_extract(report_id, sf_ma):
    sf_org = os.getenv("SFMA_ORG")
    export_params = '?isdtp=p1&export=1&enc=UTF-8&xf=csv'
    sf_report_url = sf_org + report_id + export_params
    response = requests.get(sf_report_url, headers=sf_ma.headers, cookies={'sid': sf_ma.session_id})
    new_report = response.content.decode('utf-8')
    report_df = pd.read_csv(StringIO(new_report))
    report_df = report_df.iloc[:-5]
    return report_df


def sfsl_extract(report_id, sf_sl):
    sf_org = os.getenv("SFSL_ORG")
    export_params = '?isdtp=p1&export=1&enc=UTF-8&xf=csv'
    sf_report_url = sf_org + report_id + export_params
    response = requests.get(sf_report_url, headers=sf_sl.headers, cookies={'sid': sf_sl.session_id})
    new_report = response.content.decode('utf-8')
    report_df = pd.read_csv(StringIO(new_report))
    return report_df

# Fonctions intéragir avec Supabase
def db_row_update(extract_row_id, supabase):
    current_utc_time = datetime.utcnow()
    current_utc_time_str = current_utc_time.strftime("%Y-%m-%d %H:%M:%S")
    update_response = supabase.table("coeffective_extracts").update({"last_refresh": current_utc_time_str}).eq("id", extract_row_id).execute()


# Vérification de l'horaire :
def is_time_between(starting_hour, ending_hour):
    local_time = datetime.now(pytz.timezone("Europe/Paris")).time()
    begin_hour_time = time(starting_hour-1, 59)
    end_hour_time = time(ending_hour, 29)
    if begin_hour_time < end_hour_time:
        return local_time >= begin_hour_time and local_time <= end_hour_time
    else: # crosses midnight
        return local_time >= begin_hour_time or local_time <= end_hour_time


# Vérification de la fréquence :
def frequency_check(frequency, last_refresh_date_str):
    if not last_refresh_date_str:
        return True
    if frequency == "Toutes les 30 minutes":
        min_interval = 1
    elif frequency == "Toutes les heures":
        min_interval = 45
    else:
        min_interval = 1425
    current_utc_time = datetime.now(pytz.UTC)
    date_dt = datetime.fromisoformat(last_refresh_date_str)
    time_difference = current_utc_time - date_dt
    minutes_difference = time_difference.total_seconds() / 60
    return minutes_difference > min_interval