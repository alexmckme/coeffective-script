import os
import json
from dotenv import load_dotenv
from supabase import create_client

import numpy as np
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

# Load environment variables
load_dotenv()

# Connexion aux SF
sf_ma = Salesforce(username=os.getenv("LOGMA"), password=os.getenv("PASMA"), security_token=os.getenv("TOKMA"))
sf_sl = Salesforce(username=os.getenv("LOGSL"), password=os.getenv("PASSL"), security_token=os.getenv("TOKSL"))

# Connexion à Supabase
supabase = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_ANON_KEY"))

# Connexion à Google Drive
gsa = gspread.service_account_from_dict(json.loads(os.getenv("GOOGLE_COEFFECTIVE2_CREDS")))

response_coeffective_users = supabase.table("coeffective_users").select("*").execute()
response_coeffective_updatetabs = supabase.table("coeffective_updatetabs").select("*").execute()
response_coeffective_extracts = supabase.table("coeffective_extracts").select("*").execute()

for user in response_coeffective_users.data:
    for updatetab in response_coeffective_updatetabs.data:
        if updatetab["user_id"] == user["id"]:
            gsheet_id = updatetab["gsheet_id"]
            gsheet_file = gsa.open_by_key(gsheet_id)
            updatetab_to_update = gsheet_file.get_worksheet_by_id(updatetab["updatetab_id"])
            updatetab_data_list = []

            for extract in response_coeffective_extracts.data:
                if extract["gsheet_id"] == updatetab["gsheet_id"]:

                    # Check : vérification plage horaire et vérification de la fréquence
                    check_timeframe = helpers.is_time_between(extract["starting_hour"], extract["ending_hour"])
                    check_frequency = helpers.frequency_check(extract["frequency"], extract["last_refresh"])
                    if (not check_timeframe) or (not check_frequency):
                        updatetab_data_list.append([extract["onglet_id"], extract["extract_type"], extract["report_id"], extract["last_refresh"], "SUCCESS"])
                        continue



                    if extract["extract_type"] == "salesforce-gsl":

                        try:
                            tab_for_extract = gsheet_file.get_worksheet_by_id(extract["onglet_id"])
                            tab_for_extract.clear()
                            gd.set_with_dataframe(tab_for_extract, helpers.sfsl_extract(extract["report_id"], sf_sl))

                            helpers.db_row_update(extract["id"], supabase)
                            current_dt = datetime.now(pytz.timezone("Europe/Paris"))
                            current_dt_string = current_dt.strftime("%d/%m/%Y %H:%M:%S")
                            updatetab_data_list.append([extract["onglet_id"], extract["extract_type"], extract["report_id"], current_dt_string, "SUCCESS"])
                            time_two.sleep(1)
                        except:
                            updatetab_data_list.append([extract["onglet_id"], extract["extract_type"], extract["report_id"], "ERROR", "ERROR"])
                            continue


                    elif extract["extract_type"] == "salesforce-ma":

                        try:
                            tab_for_extract = gsheet_file.get_worksheet_by_id(extract["onglet_id"])
                            tab_for_extract.clear()
                            gd.set_with_dataframe(tab_for_extract, helpers.sfma_extract(extract["report_id"], sf_ma))

                            helpers.db_row_update(extract["id"], supabase)
                            current_dt = datetime.now(pytz.timezone("Europe/Paris"))
                            current_dt_string = current_dt.strftime("%d/%m/%Y %H:%M:%S")
                            updatetab_data_list.append([extract["onglet_id"], extract["extract_type"], extract["report_id"], current_dt_string, "SUCCESS"])
                            time_two.sleep(1)
                        except:
                            updatetab_data_list.append([extract["onglet_id"], extract["extract_type"], extract["report_id"], "ERROR", "ERROR"])
                            continue


            updatetab_data_df = pd.DataFrame(np.array(updatetab_data_list), columns=["Sheet Tab GID", "Report Type", "Report ID", "Last refesh date", "Last refresh status"])
            gd.set_with_dataframe(updatetab_to_update, updatetab_data_df)
            time_two.sleep(1)