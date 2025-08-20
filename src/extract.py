import requests
import sys
import pandas as pd

from datetime import date, datetime
from requests import Response
from settings import LocationDetails
from logger import setup_logger

location_details = LocationDetails().model_dump()
logger = setup_logger(__name__)
query_details : list[str] = ["longdeg", "longmin", "latdeg",
                             "latmin", "location", "longhemi",
                             "lathemi", "loc", "timezone",
                             "date", "Event"]

def get_sydney_uv_index_data():
    local_date_today : date = date.today()
    url : str = ("https://uvdata.arpansa.gov.au/api/uvlevel/?longitude=151.1&latitude=-34.04&date="
                 + str(local_date_today))
    try :
        response : Response = requests.get(url)
        response.raise_for_status()
        json = response.json()
        return json
    except Exception as err:
        logger.error(f"An error occurred in getting uv index data: {err}")
        sys.exit(1)
    # print(get_sydney_uv_index_data.__name__)


def create_query_string() -> str:
    res = ""
    for detail in query_details:
        res += detail + "="
        if detail == "date":
            res += str(date.today().year)
        elif detail == "loc":
            res += "yes"
        elif detail == "Event":
            res += str(1)
            break
        else:
            res += location_details[detail]
        res += "&"
    return res

def get_sunrise_sunset_times():
    request = {"type":"sunrisenset",
               "query": create_query_string()}
    url = "https://api.geodesyapps.ga.gov.au/astronomical/submitRequest"
    try:
        response : Response = requests.post(url, json = request)
        response.raise_for_status()
        time_json = response.json()
        return time_json
    except Exception as err:
        logger.error(f"An error occurred in getting sunrise and sunset times: {err}")
        sys.exit(1)

def main():
    print(datetime.now())
    uv_data = get_sydney_uv_index_data()
    uv_graph_df = pd.DataFrame(uv_data['GraphData'])
    uv_graph_df = uv_graph_df.drop(columns=['$id'])
    uv_graph_df['Date'] = pd.to_datetime(uv_graph_df['Date'])
    uv_table_df = pd.DataFrame(uv_data['TableData'])
    uv_table_df = uv_table_df.drop(columns=['$id'])
    uv_graph_df.to_excel('test.xlsx', index=False)


    for key, item in uv_data.items():
        print(key, item)

if __name__ == "__main__":
    main()