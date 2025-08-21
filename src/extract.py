import requests
import sys

from datetime import date
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


def create_query_string() -> str:
    res = ""
    for detail in query_details:
        res += detail + "="
        if detail == "date":
            res += str(date.today().year)
        else:
            res += location_details[detail]
        res += "&"
    return res[:-1]

def get_sunrise_sunset_times():
    request = {"type":"sunrisenset",
               "query": create_query_string()}
    url = "https://api.geodesyapps.ga.gov.au/astronomical/submitRequest"
    try:
        response : Response = requests.post(url, json = request)
        response.raise_for_status()
        time_json = response.json()
        list_of_time_jsons = time_json['response']['events'][0]['data']
        return list_of_time_jsons
    except Exception as err:
        logger.error(f"An error occurred in getting sunrise and sunset times: {err}")
        sys.exit(1)