import requests
import datetime
from datetime import date
from requests import Response
from settings import LocationDetails

location_details = LocationDetails().model_dump()

def get_sydney_uv_index_data():
    local_date_today : date = date.today()
    url : str = ("https://uvdata.arpansa.gov.au/api/uvlevel/?longitude=151.1&latitude=-34.04&date="
                 + str(local_date_today))
    response : Response = requests.get(url)
    json = response.json()
    print(get_sydney_uv_index_data.__name__)
    return json

def get_sunrise_sunset_times():
    current_year = datetime.date.today().year
    request = {"type":"sunrisenset",
               "query":"longdeg=" + location_details['longdeg'] + "&"
                       "longmin=" + location_details['longmin'] + "&"
                       "latdeg="+ location_details['latdeg'] + "&"
                       "latmin=" + location_details['latmin'] + "&"
                       "location=" + location_details['location'] + "&"
                       "longhemi=" + location_details['longhemi'] + "&"
                       "lathemi=" + location_details['lathemi'] + "&"
                       "loc=yes&"
                       "timezone=" + location_details['longdeg'] + "&"
                       "date=" + str(current_year)+ "&"
                       "Event=1"}
    url = "https://api.geodesyapps.ga.gov.au/astronomical/submitRequest"
    response = requests.post(url, json = request)
    time_json = response.json()
    return time_json

def main():
    print(get_sydney_uv_index_data())
    print(get_sunrise_sunset_times())

if __name__ == "__main__":
    main()