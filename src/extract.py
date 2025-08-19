import requests
from datetime import date

def get_sydney_uv_index_data():
    local_date_today = date.today()
    url = ("https://uvdata.arpansa.gov.au/api/uvlevel/?longitude=151.1&latitude=-34.04&date="
                 + str(local_date_today))
    response = requests.get(url)
    json = response.json()
    print(get_sydney_uv_index_data.__name__)
    return json

def main():
    print(get_sydney_uv_index_data())

if __name__ == "__main__":
    main()