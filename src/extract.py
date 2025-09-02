import requests
import sys
import datetime as dt
import pandas as pd
import xml.etree.ElementTree as ET

from requests import Response
from settings import LocationDetails, scrape
from logger import setup_logger
from ftplib import FTP
from io import BytesIO, StringIO
from pathlib import Path

location_details = LocationDetails().model_dump()
scrape_details = scrape().model_dump()
logger = setup_logger(__name__)
query_details : list[str] = ["longdeg", "longmin", "latdeg",
                             "latmin", "location", "longhemi",
                             "lathemi", "loc", "timezone",
                             "date", "Event"]
ftp_server : str = "ftp.bom.gov.au"
resources_path = Path(__file__).parent.parent/'resources'
radar_transparencies_files = ['IDR.legend.0.png', 'IDR714.background.png', 'IDR714.locations.png', 'IDR714.topography.png']

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

def create_query_string(year : int) -> str:
    res = ""
    for detail in query_details:
        res += detail + "="
        if detail == "date":
            res += str(year)
        else:
            res += location_details[detail]
        res += "&"
    return res[:-1]

def get_sunrise_sunset_times_dataframe(year : int) -> pd.DataFrame:
    request = {"type":"sunrisenset",
               "query": create_query_string(year)}
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
        except Exception as err:
            logger.error(f"An error occurred in getting the forecast file: {err}")
            sys.exit(1)


def parse_forecast_xml() -> pd.DataFrame:
    tree = get_forecast_xml()
    root = tree.getroot()
    forecasts = root.find('forecast')
    forecast_area : ET.Element = ET.Element('None')
    for area in forecasts:
        if area.attrib['description'] == scrape_details['station']:
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


def get_hourly_observation_df() -> pd.DataFrame:
    try:
        r = requests.get(scrape_details['station_observation_url'])
        r.raise_for_status()
        observations= r.json()['observations']['data']
        df : pd.DataFrame = pd.DataFrame(observations)
        df : pd.DataFrame = df[['local_date_time_full', 'apparent_t', 'cloud',
                 'delta_t','gust_kmh','air_temp',
                 'dewpt', 'rain_trace', 'rel_hum',
                 'weather','wind_dir','wind_spd_kmh']]
        return df
    except Exception as err:
        logger.error(f'An error occurred in getting observation data: {err}')
        sys.exit(1)


def get_radar_images():
    ftp_directory : str = '/anon/gen/radar'
    path = resources_path/'radar'
    path.mkdir(parents=True,exist_ok=True)
    radar_product = 'IDR714'
    with FTP(ftp_server) as ftp:
        ftp.login()
        ftp.cwd(ftp_directory)
        image_list = [image for image in ftp.nlst() if radar_product in image and 'png' in image]
        try:
            for image in image_list:
                temp_path = path/image
                file = temp_path.open('wb')
                ftp.retrbinary(f'RETR {image}', file.write)
                file.close()
        except Exception as err:
            logger.error(f'An error occurred in getting the radar file : {err}')

def get_radar_transparencies():
    ftp_directory : str = '/anon/gen/radar_transparencies'
    path = resources_path/'radar_transparencies'
    path.mkdir(parents=True,exist_ok=True)
    with FTP(ftp_server) as ftp:
        ftp.login()
        ftp.cwd(ftp_directory)
        try:
            for image in radar_transparencies_files:
                temp_path = path/image
                file = temp_path.open('wb')
                ftp.retrbinary(f'RETR {image}', file.write)
                file.close()
        except Exception as err:
            logger.error(f'An error occurred in getting the radar transparencies : {err}')


def get_daily_observation_df(year_month : str) -> pd.DataFrame:
    ftp_directory : str = scrape_details['daily_observation_directory']
    df : pd.DataFrame = pd.DataFrame()
    with FTP(ftp_server) as ftp:
        ftp.login()
        ftp.cwd(ftp_directory)
        file_list = ftp.nlst()[::-1][1:]
        for file_name in file_list:
            file_date = file_name[-10:-4]
            if year_month > file_date:
                break
            str_io = StringIO()
            reader = BytesIO()
            try:
                ftp.retrbinary(f'RETR {file_name}', reader.write)
                file_str = reader.getvalue().decode('utf-8', errors='ignore')
                str_io.write(file_str[file_str.index('Station'):])
                str_io.seek(0)
                temp_df : pd.DataFrame = pd.read_csv(str_io)
                temp_df : pd.DataFrame = temp_df.dropna()
                df = pd.concat([temp_df, df])
            except Exception as err:
                logger.error(f'An error occurred in getting daily observation file: {err}')
                sys.exit(1)
    return df

# def main():
#
# if __name__ == "__main__":
#     main()