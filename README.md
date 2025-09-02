# WeatherData

## Project Overview
The project is an ETL pipeline that takes weather observation data, UV index data, and sunrise and sunset data.
The data is extracted from APIs or an FTP server, cleaned, transformed, and loaded into an Excel file. The Excel file 
is then read for a Tableau dashboard. 

## Usage
Two env files need to be created named '.locationdetails.env' and '.scrape.env'.

'.locationdetails.env' is required to get sunrise/sunset times from [https://geodesyapps.ga.gov.au/sunrise](https://geodesyapps.ga.gov.au/sunrise).
The following details are required
```
    longdeg
    longmin
    latdeg
    location
    longhemi
    lathemi
    timezeon
    loc
    Event
```
Exact details of a specific location can be found:
1. Searching for a location at the top of the website
2. Searching for events for that location
3. Examining the requests in the network tab, found in the devtools of a web browser. Open the devtools before events are searched for 

'.scrape.env' is required to get daily and hourly observation data from weather stations
The following details are required
```
    station_observation_url
    daily_observation_directory
    station
```
station_observation_url refers to url links to hourly station observations in json formats. 
All weather stations for New South Wales can be found at [https://reg.bom.gov.au/nsw/observations/nswall.shtml](https://reg.bom.gov.au/nsw/observations/nswall.shtml).
Json file links can be found at the bottom of the page for a specific station.

daily_observation_directory refers to a directory, for a specific station, found at the [Bureau of Meteorology's FTP server](ftp://ftp.bom.gov.au).
The directory which contains all stations can be found in [here](ftp://ftp.bom.gov.au/anon/gen/clim_data/IDCKWCDEA0/tables).
daily_observation_directory start with '/anon'. Use an FTP client to find a specific file such as FileZill/WinSCP. A full list 
of data that can be found in the FTP servers can be found at [here](https://www.bom.gov.au/catalogue/data-feeds.shtml).

station refers to the name of that station found in the précis forecast which can be found [here](https://www.bom.gov.au/catalogue/data-feeds.shtml).
Specific names can be found as a description attribute for an area element of the XML file.

### Note on cron jobs
Specify the entire file path of the env files and the Excel file, or the cron job will run into an error.

## Future Improvements
- Enable the selection of weather stations
- Incorporate radar images into the dashboard.


## License
This project is licensed under the MIT License. See the LICENSE file for details
