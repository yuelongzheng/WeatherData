import requests
import sys
import datetime as dt
import pandas as pd
import xml.etree.ElementTree as ET
import json
from requests import Response
from settings import LocationDetails
from logger import setup_logger
from ftplib import FTP
from io import BytesIO

location_details = LocationDetails().model_dump()
logger = setup_logger(__name__)
query_details : list[str] = ["longdeg", "longmin", "latdeg",
                             "latmin", "location", "longhemi",
                             "lathemi", "loc", "timezone",
                             "date", "Event"]

def get_sydney_uv_index_data(date : dt.date) -> pd.DataFrame:
    url : str = ("https://uvdata.arpansa.gov.au/api/uvlevel/?longitude=151.1&latitude=-34.04&date="
                 + str(date))
    try :
        response : Response = requests.get(url)
        response.raise_for_status()
        response_json = response.json()
        graph_data = response_json['GraphData']
        return pd.DataFrame(graph_data)
    except Exception as err:
        logger.error(f"An error occurred in getting uv index data: {err}")
        sys.exit(1)

def get_uv_index_dataframe(datetime_index : pd.DatetimeIndex) -> pd.DataFrame:
    scrape_df : pd.DataFrame = pd.DataFrame()
    for timestamp in datetime_index:
        tmp : pd.DataFrame = get_sydney_uv_index_data(timestamp.date())
        scrape_df : pd.DataFrame = tmp if scrape_df.empty else pd.concat([scrape_df, tmp])
    return scrape_df

def create_query_string() -> str:
    res = ""
    for detail in query_details:
        res += detail + "="
        if detail == "date":
            res += str(dt.date.today().year)
        else:
            res += location_details[detail]
        res += "&"
    return res[:-1]

def get_sunrise_sunset_times_dataframe() -> pd.DataFrame:
    request = {"type":"sunrisenset",
               "query": create_query_string()}
    url = "https://api.geodesyapps.ga.gov.au/astronomical/submitRequest"
    try:
        response : Response = requests.post(url, json = request)
        response.raise_for_status()
        time_json = response.json()
        list_of_time_jsons = time_json['response']['events'][0]['data']
        return pd.DataFrame(list_of_time_jsons)
    except Exception as err:
        logger.error(f"An error occurred in getting sunrise and sunset times: {err}")
        sys.exit(1)

def get_forecast_xml() -> ET.ElementTree:
    ftp_server : str = "ftp.bom.gov.au"
    ftp_directory : str = "/anon/gen/fwo"
    sydney_forecast_file : str = 'IDN11060.xml'
    with FTP(ftp_server) as ftp:
        ftp.login()
        ftp.cwd(ftp_directory)
        reader = BytesIO()
        try:
            ftp.retrbinary(f'RETR {sydney_forecast_file}', reader.write)
            forecast_str = reader.getvalue().decode('utf-8')
            return ET.ElementTree(ET.fromstring(forecast_str))
        except Exception as e:
            logger.error(f"An error occurred in getting the forecast file: {e}")
            sys.exit(1)


def parse_forecast_xml() -> pd.DataFrame:
    tree = get_forecast_xml()
    root = tree.getroot()
    forecasts = root.find('forecast')
    forecast_area : ET.Element = ET.Element('None')
    for area in forecasts:
        if area.attrib['description'] == location_details['station']:
            forecast_area = area
    list_of_dict = []
    for forecast_period in forecast_area:
        temp_dict = forecast_period.attrib
        for forecast in forecast_period:
            temp_dict[forecast.attrib['type']] = forecast.text
        list_of_dict.append(temp_dict)
    df = pd.DataFrame(list_of_dict)
    drop_columns = ['index', 'start-time-utc', 'end-time-utc',
                    'forecast_icon_code']
    df = df.drop(columns=drop_columns)
    return df

def get_observation_json():
    with open('IDN60901.95765.json', 'r') as file:
        observations = json.load(file)
        df = pd.DataFrame(observations['observations']['data'])
        df = df[['local_date_time_full', 'apparent_t', 'cloud',
                 'delta_t','gust_kmh','air_temp',
                 'dewpt', 'rain_trace', 'rel_hum',
                 'weather','wind_dir','wind_spd_kmh']]
        print(df)


def main():
    df = parse_forecast_xml()
    for column in df.columns:
        print(df[column])
    # get_observation_json()

if __name__ == "__main__":
    main()