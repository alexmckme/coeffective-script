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
import tableauserverclient as TSC
from zipfile import ZipFile
import glob
import pantab as pt
from tableauhyperapi import TableName
import shutil

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

    if user["tableau_personal_token_name"]:
        try:
            tableau_auth = TSC.PersonalAccessTokenAuth(user["tableau_personal_token_name"],
                                                       user["tableau_personal_token_value"],
                                                       site_id='avivkugawana')
            server = TSC.Server('https://eu-west-1a.online.tableau.com/', use_server_version=True)

            req_option_user = TSC.RequestOptions(pagesize=1000)
            req_option_user.filter.add(TSC.Filter(TSC.RequestOptions.Field.OwnerName,
                                                  TSC.RequestOptions.Operator.Equals,
                                                  user["tableau_user_full_name"]))

        except Exception as error:
            if isinstance(error, TSC.server.endpoint.NotSignedInError):
                print("Problème avec le token Tableau")
            print(error)

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

                        except:
                            updatetab_data_list.append([extract["onglet_id"], extract["extract_type"], extract["report_id"], "ERROR", "ERROR"])
                            continue

                    elif extract["extract_type"] == "flamingo":

                        try:

                            with server.auth.sign_in(tableau_auth):
                                # obtenir tous les datasources Tableau dont je suis propriétaire
                                datasource_list, information = server.datasources.get(req_option_user)

                                for datasource in datasource_list:
                                    if extract["report_id"] == datasource.name:
                                        # télécharger l'extract
                                        server.datasources.download(datasource.id, filepath="to_dezip_to_hyper")
                                        # dézipper l'extract
                                        with ZipFile("to_dezip_to_hyper.tdsx") as zip_ref:
                                            zip_ref.extractall("dezipped_folder")

                                            # trouver le path du fichier
                                            cwd = os.getcwd()
                                            extract_dir_path = "dezipped_folder/Data/Extracts/*"
                                            extract_file_path = glob.glob(os.path.join(cwd, extract_dir_path))[0]

                                            # transformer le hyper en dataframe
                                            extracted_df = pt.frame_from_hyper(extract_file_path,
                                                                       table=TableName("Extract", "Extract"))

                                            tab_for_extract = gsheet_file.get_worksheet_by_id(extract["onglet_id"])
                                            tab_for_extract.clear()
                                            gd.set_with_dataframe(tab_for_extract, extracted_df)

                                        # supprimer les tdsx et dezipped_folder
                                        os.remove("to_dezip_to_hyper.tdsx")
                                        shutil.rmtree("dezipped_folder")

                                        helpers.db_row_update(extract["id"], supabase)
                                        current_dt = datetime.now(pytz.timezone("Europe/Paris"))
                                        current_dt_string = current_dt.strftime("%d/%m/%Y %H:%M:%S")
                                        updatetab_data_list.append(
                                        [extract["onglet_id"], extract["extract_type"], extract["report_id"], current_dt_string,
                                        "SUCCESS"])

                        except Exception as error:
                            if isinstance(error, TSC.server.endpoint.NotSignedInError):
                                print("Problème avec le token Tableau")
                            print(error)
                            updatetab_data_list.append([extract["onglet_id"], extract["extract_type"], extract["report_id"], "ERROR", "ERROR"])
                            continue

            updatetab_data_df = pd.DataFrame(np.array(updatetab_data_list), columns=["Sheet Tab GID", "Report Type", "Report ID", "Last auto refesh date", "Last auto refresh status"])
            gd.set_with_dataframe(updatetab_to_update, updatetab_data_df)
            time_two.sleep(1)