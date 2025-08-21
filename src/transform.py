import pandas as pd

from extract import get_sydney_uv_index_data, get_sunrise_sunset_times
from datetime import date, datetime, timedelta

current_year = date.today().year
spreadsheet_name = "WeatherData.xlsx"
sheetnames : dict[str,str] = {"uv" : "UV Index Data",
                              "sunrise_sunset_times" : str(current_year) + " Sunrise Sunset Times" }
sunrise_sunset_columns = {'month':"Month", 'day':'Day', 'rise':'Rise',
                          'rise_day':'Rise_Day', 'set':'Set', 'set_day':'Set_Day'}
def check_spreadsheet_exists():
    try:
        pd.read_excel(spreadsheet_name)
    except Exception as err:
        empty = pd.DataFrame()
        with pd.ExcelWriter(spreadsheet_name) as writer:
            for sheetname in sheetnames.values():
                empty.to_excel(writer, sheet_name=sheetname)

def update_uv_index_data():
    uv_data = get_sydney_uv_index_data()
    uv_graph_df = pd.DataFrame(uv_data['GraphData'])
    uv_graph_df = uv_graph_df.drop(columns=['$id'])
    uv_graph_df['Date'] = pd.to_datetime(uv_graph_df['Date'])
    current_uv_graph_df = pd.read_excel(spreadsheet_name, sheet_name=sheetnames['uv'])
    last_row = current_uv_graph_df.shape[0] - 1

    if last_row == -1:
        uv_graph_df.to_excel(spreadsheet_name, sheet_name=sheetnames['uv'], index = False)
        return None

    last_time = current_uv_graph_df['Date'][last_row]
    first_time = uv_graph_df['Date'][0]
    if first_time > last_time:
        current_uv_graph_df = pd.concat([current_uv_graph_df,uv_graph_df])
    else:
        first_na_value_index = current_uv_graph_df['Measured'].isna().idxmax()
        cutoff_date = current_uv_graph_df.loc[first_na_value_index]['Date']
        uv_graph_df = pd.DataFrame(uv_graph_df[uv_graph_df['Date'] >= cutoff_date])
        uv_graph_rows = uv_graph_df.shape[0]
        new_index = list(range(first_na_value_index, first_na_value_index + uv_graph_rows))
        uv_graph_df.index = new_index
        current_uv_graph_df = current_uv_graph_df.combine_first(uv_graph_df)
    current_uv_graph_df.to_excel(spreadsheet_name, sheet_name = sheetnames['uv'], index = False)
    return None

def get_first_sunday(year, month):
    day, sunday = 7, 6
    # day = 7, then all possible days have passed
    tmp = datetime(year, month, day)
    current_day = tmp.weekday()
    offset = -((current_day - sunday) % day)
    return pd.to_datetime((tmp + timedelta(offset)).date())

def convert_to_date_time(df, col):
    df[col] = df[col].str[:2] + ":" + df[col].str[2:4]
    df[col] = pd.to_datetime(df[col], format="%H:%M").dt.strftime('%I:%M %p')
    df[col] = pd.to_datetime((df['Date'].dt.strftime('%d/%m/%Y') + " " +  df[col]), format = '%d/%m/%Y %I:%M %p')

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
    time_df = time_df.drop(columns = ['Month', 'Day', 'Rise_Day', 'Set_Day', 'Year', 'Date'])
    print(time_df)
    print(time_df.dtypes)
    # time_df.to_excel("test.xlsx", sheet_name= sheetnames['sunrise_sunset_times'], index = False)
    # time_df.to_excel(spreadsheet_name, sheet_name= sheetnames['sunrise_sunset_times'], index = False)


def load_sunrise_sunset_times():
    file = pd.ExcelFile(spreadsheet_name)
    current_sheetnames = file.sheet_names
    sheet_exists = False
    # if sheetnames['sunrise_sunset_times'] in current_sheetnames:
    #     sheet_exists = True
    # if not sheet_exists:




def main():
    get_sunrise_sunset_time_df()
    # check_spreadsheet_exists()
    # update_uv_index_data()
    # get_sunrise_sunset_time_df()

if __name__ == "__main__":
    main()