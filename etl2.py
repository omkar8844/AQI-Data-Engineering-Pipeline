import pandas as pd
import requests
import os
import ast
from sqlalchemy import create_engine, text
import psycopg2
from prefect import flow, task
from prefect import scheduling
from datetime import timedelta

# Extract Function
def get_data(cities):
    dfs = []
    if type(cities) == str:
        cities = [cities]
        print(cities)
    for city in cities:
        url = f'URL WITH API-TOKEN'
        r = requests.get(url)
        json_data = r.json()
        if json_data['status'] == 'error':
            print(f'This city does not exist in the API data: {city}')
            continue
        dff = pd.json_normalize(json_data)
        dff.columns = dff.columns.str.replace('data.', '', regex=False)
        dff.columns = dff.columns.str.replace('.', '_', regex=False)
        dff['city'] = dff['city_url'].str.extract(r'([^/]+)$')
        dfs.append(dff)
    fdf = pd.concat(dfs, ignore_index=True)
    fdf['idx_index'] = fdf['idx'].astype(str) + '_' + fdf['time_s']
    return fdf

# Transform Function
def return_dfs(df):
    # Transformation
    df['time_s'] = pd.to_datetime(df['time_s'])
    
    def normalize_forecast(value):
        if isinstance(value, str):
            return ast.literal_eval(value)
        elif isinstance(value, list):
            return value
        else:
            return []

    def append_nested(fdf, colname):
        nested_dfs = []
        for i in range(len(fdf)):
            edf = pd.DataFrame(df[f'{colname}'].apply(normalize_forecast)[i])
            edf['idx_index'] = fdf['idx_index'].iloc[i]
            edf.drop_duplicates('idx_index', keep='first', inplace=True)
            nested_dfs.append(edf)
        return pd.concat(nested_dfs, ignore_index=True)
    
    forecast_daily_pm10 = append_nested(df, 'forecast_daily_pm10')
    forecast_daily_pm25 = append_nested(df, 'forecast_daily_pm25')
    forecast_daily_uvi = append_nested(df, 'forecast_daily_uvi')
    forecast_daily_o3 = append_nested(df, 'forecast_daily_o3')
    
    # Dropping unnecessary columns
    drop_list = ['forecast_daily_o3', 'forecast_daily_pm10', 'forecast_daily_pm25', 'forecast_daily_uvi', 'status', 'attributions', 'city_geo', 'city_url']
    df = df.drop(drop_list, axis=1)
    
    # Dividing columns into different tables
    df['time_primary_key'] = df['time_s'].astype(str) + '_' + df['time_tz'] + '_' + df['time_v'].astype(str)
    city_table = df[['idx', 'city_name', 'city']].drop_duplicates('idx', keep='first')
    time_table = df[['time_primary_key', 'time_s', 'time_tz', 'time_v', 'time_iso']].drop_duplicates()
    air_q_table = df[['idx_index', 'time_primary_key', 'idx', 'aqi', 'dominentpol', 'iaqi_h_v', 'iaqi_o3_v', 'iaqi_p_v', 'iaqi_pm10_v', 'iaqi_pm25_v', 'iaqi_t_v', 'iaqi_w_v', 'iaqi_no2_v']]
    
    return {
        'airQtable': air_q_table,
        'city_table': city_table,
        'time_table': time_table,
        'forecast_daily_o3': forecast_daily_o3,
        'forecast_daily_pm10': forecast_daily_pm10,
        'forecast_daily_pm25': forecast_daily_pm25,
        'forecast_daily_uvi': forecast_daily_uvi
    }

# Load Function
def load_to_sql(sqldf, table_name, index_column):
    postgres_uri = 'postgresql://omkar:omkar@localhost:5432/aqi'
    engine = create_engine(postgres_uri)
    
    with engine.connect() as connection:
        if engine.dialect.has_table(connection, table_name):
            print(f'Table {table_name} already exists')
            query = f'SELECT {index_column} FROM "{table_name}"'
            existing_index = pd.read_sql(query, con=engine)[f'{index_column}'].tolist()
            filtered_df = sqldf[~(sqldf[f'{index_column}'].isin(existing_index))]
            print(f"Filtered data to {len(filtered_df)} rows to avoid duplicates.")
        else:
            print(f"Table '{table_name}' does not exist. Creating a new table.")
            filtered_df = sqldf
        if not filtered_df.empty:
            filtered_df.to_sql(table_name, if_exists='append', index=False, con=engine)
            print(f"Appended {len(filtered_df)} rows to the table '{table_name}'.")
        else:
            print("No new rows to append to the table.")

# Loading into postgres
def load_df_list(df_list):
    index_columns = {
        'airQtable': 'idx_index',
        'city_table': 'idx',
        'time_table': 'time_primary_key',
        'forecast_daily_o3': 'idx_index',
        'forecast_daily_pm10': 'idx_index',
        'forecast_daily_pm25': 'idx_index',
        'forecast_daily_uvi': 'idx_index' 
    }
    for table_name, sqldf in df_list.items():
        print(f'Loading {table_name} into postgres table {table_name} with primary key as {index_columns[table_name]}')
        load_to_sql(sqldf, table_name, index_columns[table_name])

# List of cities
city_list = ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix', 'Philadelphia', 
             'San Antonio', 'San Diego', 'Dallas', 'San Jose', 'Austin', 'Jacksonville', 
             'Fort Worth', 'Columbus', 'San Francisco', 'Charlotte', 'Indianapolis', 
             'Seattle', 'Denver', 'Washington, D.C.', 'Boston', 'El Paso', 'Nashville', 
             'Detroit', 'Oklahoma City', 'Portland', 'Las Vegas', 'Memphis', 'Louisville', 
             'Baltimore', 'Milwaukee', 'Albuquerque', 'Tucson', 'Fresno', 'Sacramento', 
             'Mesa', 'Kansas City', 'Atlanta', 'Omaha', 'Raleigh', 'Miami', 'Long Beach', 
             'Virginia Beach', 'Oakland', 'Minneapolis','Tulsa', 'Arlington', 'Tampa', 
             'New Orleans', 'Wichita', 'Cleveland']

# Main function
@task
def get_city_data(cities=city_list):
    df = get_data(cities)
    df_list = return_dfs(df)
    load_df_list(df_list)

# Interval schedule to run every 4 hours
schedule = scheduling.Interval(interval=timedelta(hours=4))

# Flow with the scheduled task
@flow(schedule=schedule)
def scheduled_flow():
    get_city_data()

# Run the flow (Prefect takes care of scheduling, so no need to call `scheduled_flow()` explicitly)
if __name__ == "__main__":
    scheduled_flow()
