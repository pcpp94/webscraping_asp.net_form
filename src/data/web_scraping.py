import os
import pandas as pd
import datetime

from ..client.pe_client import pe_Client
from ..config import COMPILED_OUTPUTS_DIR

# Dates
end = datetime.date.today() - datetime.timedelta(days=2)

outputs = [x for x in os.listdir(COMPILED_OUTPUTS_DIR) if x[-4:] != ".csv"]
max_dates = []
for file in outputs:
    df = pd.read_parquet(os.path.join(COMPILED_OUTPUTS_DIR, file))
    if df["date"].dtype.name == "datetime64[ns]":
        max_dates.append(df["date"].max())
    else:
        df["date"] = pd.to_datetime(df["date"], format="%d/%m/%Y")
        max_dates.append(df["date"].max())
start = min(max_dates)


dates_to_parse = pd.date_range(start=start, end=end, freq="d")
dates_to_parse = dates_to_parse.strftime("%d/%m/%Y").to_list()
reversed_dates = list(reversed(dates_to_parse))

# URLs
root_url = "http://coes.sin/reportes/"
login_page_url = "http://coes.sin/reportes/Acceso.aspx"
login_action_url = "http://coes.sin/reportes/Inicio.aspx"


def parse_new_data_from_new_to_old():

    pe_client = pe_Client(root_url, login_page_url, login_action_url)
    pe_client.get_date_state(reversed_dates[0])
    pe_client.get_all_raw_reports()
    for scrape_date in reversed_dates[1:]:
        pe_client.get_date_state(scrape_date)
        pe_client.get_all_raw_reports()


def parse_new_data_from_old_to_new():

    pe_client = pe_Client(root_url, login_page_url, login_action_url)
    pe_client.get_date_state(dates_to_parse[0])
    pe_client.get_all_raw_reports()
    for scrape_date in dates_to_parse[1:]:
        pe_client.get_date_state(scrape_date)
        pe_client.get_all_raw_reports()
