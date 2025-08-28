import pandas as pd
import datetime as dt

from extract import get_sydney_uv_index_data, get_uv_index_dataframe, get_sunrise_sunset_times_dataframe, parse_forecast_xml
from logger import setup_logger
from settings import LocationDetails

current_date : dt.date = dt.date.today()
spreadsheet_name : str = "WeatherData.xlsx"
sheet_names : dict[str,str] = {"uv" : "UV Index Data",
                              "sunrise_sunset_times" : "Sunrise Sunset Times",
                               'forecast' : 'Forecast'}
sunrise_sunset_columns : dict[str,str] = {'month':"Month", 'day':'Day', 'rise':'Rise',
                                          'rise_day':'Rise_Day', 'set':'Set', 'set_day':'Set_Day'}
logger = setup_logger(__name__)
location_details = LocationDetails().model_dump()

def check_spreadsheet_exists():
    empty = pd.DataFrame()
    try:
        file = pd.ExcelFile(spreadsheet_name)
        current_sheet_names = file.sheet_names
        add = []
        for sheet_name in sheet_names.values():
            if sheet_name not in current_sheet_names:
                add.append(sheet_name)
        for sheet_name in add:
            write_to_excel(spreadsheet_name, sheet_name, empty)
    except Exception as err:
        with pd.ExcelWriter(spreadsheet_name) as writer:
            for sheet_name in sheet_names.values():
                empty.to_excel(writer, sheet_name=sheet_name)

def transform_uv_index_df(df : pd.DataFrame):
    df = df.drop(columns=['$id'])
    df['DateTime'] = pd.to_datetime((df['Date']))
    df['Date'] = pd.to_datetime(df['DateTime'].dt.date)
    return df


def update_uv_index_data():
    current_uv_graph_df : pd.DataFrame = pd.read_excel(spreadsheet_name, sheet_name=sheet_names['uv'])
    today_date : dt.date = dt.date.today()
    if not current_uv_graph_df.empty:
        first_na_index : int = current_uv_graph_df['Measured'].isna().idxmax()
        start_dateTime : pd.Timestamp = current_uv_graph_df.loc[first_na_index]['DateTime'] if first_na_index != 0 else \
                                         current_uv_graph_df['Date'].iat[-1] + pd.Timedelta(days=1)
        start_date : dt.date = start_dateTime.date()
        datetime_index : pd.DatetimeIndex = pd.date_range(start_date, today_date)
        scrape_df = get_uv_index_dataframe(datetime_index)
        scrape_df = transform_uv_index_df(scrape_df)
        scrape_df = pd.DataFrame(scrape_df[scrape_df['DateTime'] >= start_dateTime])
        scrape_df.index = list(range(first_na_index, first_na_index + scrape_df.shape[0])) if first_na_index != 0 else \
                          list(range(current_uv_graph_df.shape[0], current_uv_graph_df.shape[0] + scrape_df.shape[0]))
        current_uv_graph_df = current_uv_graph_df.combine_first(scrape_df)
    else:
        current_uv_graph_df = get_sydney_uv_index_data(today_date)
        current_uv_graph_df = transform_uv_index_df(current_uv_graph_df)
    write_to_excel(spreadsheet_name, sheet_names['uv'], current_uv_graph_df)


def get_first_sunday(year : int, month : int):
    day, sunday = 7, 6
    # day = 7, then all possible days have passed
    tmp = dt.datetime(year, month, day)
    current_day = tmp.weekday()
    offset = -((current_day - sunday) % day)
    return pd.to_datetime((tmp + dt.timedelta(offset)).date())


def convert_to_date_time(df : pd.DataFrame, col : str):
    df[col] = df[col].str[:2] + ":" + df[col].str[2:4]
    df[col] = pd.to_datetime((df['Date'].dt.strftime('%d/%m/%Y') + " " +  df[col]), format = '%d/%m/%Y %H:%M')


def transform_aedt_times(df :pd.DataFrame , mask : 'pd.Series[bool]', col : str):
    mask_df = df[mask]
    df.loc[mask, col] = mask_df[col] + dt.timedelta(hours=1)

def create_time_df(df : pd.DataFrame, col : str):
    transform_df : pd.DataFrame = df.loc[df.index, [col, 'Date']]
    transform_df['Time'] = transform_df[col].dt.time
    transform_df['Sunrise/Sunset'] = 'Sun' + col.lower()
    transform_df = transform_df.drop(columns=[col])
    return transform_df

def transform_sunrise_sunset_time_df(time_df : pd.DataFrame, year : int) -> pd.DataFrame:
    time_df = time_df.rename(columns=sunrise_sunset_columns)
    time_df['Year'] = year
    time_df['Date'] = pd.to_datetime(time_df[['Year', 'Month', 'Day']])
    convert_to_date_time(time_df, 'Rise')
    convert_to_date_time(time_df, 'Set')
    april,october = 4, 10
    first_april_sunday = get_first_sunday(current_date.year,april)
    first_october_sunday = get_first_sunday(current_date.year,october)
    aedt_mask = (time_df['Date'] < first_april_sunday) | (time_df['Date'] >= first_october_sunday)
    transform_aedt_times(time_df,aedt_mask,'Rise')
    transform_aedt_times(time_df,aedt_mask,'Set')
    time_df = time_df.drop(columns = ['Month', 'Day', 'Rise_Day', 'Set_Day', 'Year'])
    sunrise_df = create_time_df(time_df, 'Rise')
    sunset_df = create_time_df(time_df, 'Set')
    time_df = pd.concat([sunrise_df,sunset_df])
    return time_df


def update_sunrise_sunset_times():
    current_time_df :pd.DataFrame = pd.read_excel(spreadsheet_name, sheet_name=sheet_names['sunrise_sunset_times'])
    last_date_year : int = current_time_df['Date'].iat[-1].year if not current_time_df.empty else 0
    if last_date_year <= current_date.year:
        time_df : pd.DataFrame = pd.DataFrame()
        required_year : int = current_date.year
        if last_date_year < current_date.year:
            time_df : pd.DataFrame = get_sunrise_sunset_times_dataframe(required_year)
            time_df : pd.DataFrame  = transform_sunrise_sunset_time_df(time_df, required_year)
        if current_date.month == 12:
            next_year_time_df : pd.DataFrame = get_sunrise_sunset_times_dataframe(required_year + 1)
            next_year_time_df : pd.DataFrame = transform_sunrise_sunset_time_df(next_year_time_df, required_year + 1)
            time_df = pd.concat([time_df, next_year_time_df])
        current_time_df : pd.DataFrame = pd.concat([current_time_df, time_df])
        write_to_excel(spreadsheet_name, sheet_names['sunrise_sunset_times'],current_time_df)


def transform_forcast_df(df : pd.DataFrame) -> pd.DataFrame:
    df['Start Date Time'] = pd.to_datetime(df['start-time-local'],format='%Y-%m-%dT%H:%M:%S+' +
                                            location_details['timezone'] + ":00" )
    df['Start Date'] = pd.to_datetime(df['Start Date Time'].dt.date)
    df = df.drop(columns=['start-time-local','end-time-local', 'Start Date Time'])
    df['probability_of_precipitation'] = df['probability_of_precipitation'].str.replace('%', '')
    df = df.rename(columns={'precis' : 'Condition',
                            'probability_of_precipitation' : 'Probability of Rain',
                            'precipitation_range' : 'Precipitation Range',
                            'air_temperature_minimum' : 'Minimum Temperature',
                            'air_temperature_maximum' : 'Maximum Temperature'})
    df = df.dropna(subset=['Minimum Temperature','Maximum Temperature'])
    df = df.astype({'Probability of Rain' : 'int32',
                    'Minimum Temperature' : 'int32',
                    'Maximum Temperature' : 'int32' })
    return df

    # print(df.loc[df['Start Date'] == df.loc[df.index[-1],'Start Date']].index.values)
    # print(df.loc[df['Start Date'] == dt.date(2025,8,1)])

def update_forcast_df():
    df = parse_forecast_xml()
    df = transform_forcast_df(df)
    write_to_excel(spreadsheet_name, sheet_names['forecast'], df)

def write_to_excel(filename,sheet_name,df):
    with pd.ExcelWriter(filename, engine='openpyxl', mode='a') as writer:
        workbook = writer.book
        try:
            workbook.remove(workbook[sheet_name])
        except Exception as err:
            logger.error(f"Write to excel error: {err}")
        finally:
            df.to_excel(writer, sheet_name=sheet_name,index= False)


def main():
    check_spreadsheet_exists()
    update_uv_index_data()
    update_sunrise_sunset_times()
    update_forcast_df()

if __name__ == "__main__":
    main()