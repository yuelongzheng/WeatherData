import pandas as pd
import datetime as dt

from extract import get_uv_index_dataframe, get_sunrise_sunset_times
from datetime import date, datetime, timedelta
from logger import setup_logger

current_year = date.today().year
spreadsheet_name = "WeatherData.xlsx"
sheetnames : dict[str,str] = {"uv" : "UV Index Data",
                              "sunrise_sunset_times" : "Sunrise Sunset Times" }
sunrise_sunset_columns = {'month':"Month", 'day':'Day', 'rise':'Rise',
                          'rise_day':'Rise_Day', 'set':'Set', 'set_day':'Set_Day'}
logger = setup_logger(__name__)

def check_spreadsheet_exists():
    try:
        pd.read_excel(spreadsheet_name)
    except Exception as err:
        empty = pd.DataFrame()
        with pd.ExcelWriter(spreadsheet_name) as writer:
            for sheetname in sheetnames.values():
                empty.to_excel(writer, sheet_name=sheetname)

def update_uv_index_data():
    current_uv_graph_df : pd.DataFrame = pd.read_excel(spreadsheet_name, sheet_name=sheetnames['uv'])
    first_na_index : int = current_uv_graph_df['Measured'].isna().idxmax()
    start_dateTime : pd.Timestamp = current_uv_graph_df.loc[first_na_index]['DateTime'] if first_na_index != 0 else \
                                     current_uv_graph_df['Date'].iat[-1] + pd.Timedelta(days=1)
    start_date : dt.date = start_dateTime.date()
    today_date : dt.date = dt.date.today()
    datetime_index : pd.DatetimeIndex = pd.date_range(start_date, today_date)
    scrape_df = get_uv_index_dataframe(datetime_index)
    scrape_df = scrape_df.drop(columns=['$id'])
    scrape_df['DateTime'] = pd.to_datetime(scrape_df['Date'])
    scrape_df['Date'] = pd.to_datetime(scrape_df['DateTime'].dt.date)
    scrape_df = pd.DataFrame(scrape_df[scrape_df['DateTime'] >= start_dateTime])
    scrape_df.index = list(range(first_na_index, first_na_index + scrape_df.shape[0])) if first_na_index != 0 else \
                      list(range(current_uv_graph_df.shape[0], current_uv_graph_df.shape[0] + scrape_df.shape[0]))
    current_uv_graph_df = current_uv_graph_df.combine_first(scrape_df)
    write_to_excel(spreadsheet_name, sheetnames['uv'], current_uv_graph_df)

def get_first_sunday(year, month):
    day, sunday = 7, 6
    # day = 7, then all possible days have passed
    tmp = datetime(year, month, day)
    current_day = tmp.weekday()
    offset = -((current_day - sunday) % day)
    return pd.to_datetime((tmp + timedelta(offset)).date())

def convert_to_date_time(df, col):
    df[col] = df[col].str[:2] + ":" + df[col].str[2:4]
    df[col] = pd.to_datetime((df['Date'].dt.strftime('%d/%m/%Y') + " " +  df[col]), format = '%d/%m/%Y %H:%M')

def transform_aedt_times(df, mask, col):
    mask_df = df[mask]
    df.loc[mask, col] = mask_df[col] + timedelta(hours=1)
    return df

def get_sunrise_sunset_time_df():
    time_df = pd.DataFrame(get_sunrise_sunset_times())
    time_df = time_df.rename(columns=sunrise_sunset_columns)
    time_df['Year'] = current_year
    time_df['Date'] = pd.to_datetime(time_df[['Year', 'Month','Day']])
    convert_to_date_time(time_df, 'Rise')
    convert_to_date_time(time_df, 'Set')
    april,october = 4, 10
    first_april_sunday = get_first_sunday(current_year,april)
    first_october_sunday = get_first_sunday(current_year,october)
    aedt_mask = (time_df['Date'] < first_april_sunday) | (time_df['Date'] >= first_october_sunday)
    transform_aedt_times(time_df,aedt_mask,'Rise')
    transform_aedt_times(time_df,aedt_mask,'Set')
    time_df = time_df.drop(columns = ['Month', 'Day', 'Rise_Day', 'Set_Day', 'Year'])
    write_to_excel(spreadsheet_name, sheetnames['sunrise_sunset_times'], time_df)

def write_to_excel(filename,sheetname,df):
    with pd.ExcelWriter(filename, engine='openpyxl', mode='a') as writer:
        workbook = writer.book
        try:
            workbook.remove(workbook[sheetname])
        except Exception as err:
            logger.error(f"Write to excel error: {err}")
        finally:
            df.to_excel(writer, sheet_name=sheetname,index= False)


def load_sunrise_sunset_times():
    file = pd.ExcelFile(spreadsheet_name)
    current_sheetnames = file.sheet_names
    sheet_exists = False
    # if sheetnames['sunrise_sunset_times'] in current_sheetnames:
    #     sheet_exists = True
    # if not sheet_exists:




def main():
    check_spreadsheet_exists()
    update_uv_index_data()
    get_sunrise_sunset_time_df()

if __name__ == "__main__":
    main()