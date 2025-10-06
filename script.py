import requests 
import os 
from dotenv import load_dotenv 
import time
import json
import csv
load_dotenv()
import snowflake.connector
from datetime import datetime


# Load API key from environment variable
API_Key = os.getenv("API_Key")
LIMIT = 1000
#tickers=[]
#url=f'https://api.polygon.io/v3/reference/tickers?market=stocks&active=true&order=asc&limit={LIMIT}&sort=ticker&apiKey={API_Key}'

DS='2025-10-05'

# Snowflake connection parameters from environment variables
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_TABLE = os.getenv("SNOWFLAKE_TABLE", "TICKERS")



#Dump into CSV file 
'''
def run_stock_job():
    with open('tickers.csv', 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=csv_fields)
        writer.writeheader()
        for ticker in tickers:
            # Only write fields present in example_ticker schema
            row = {field: ticker.get(field, '') for field in csv_fields}
            writer.writerow(row)

    print("Tickers data saved to tickers.csv")
'''
def run_stock_job():
    DS=datetime.now().strftime('%Y-%m-%d')
    tickers=[]
    url=f'https://api.polygon.io/v3/reference/tickers?market=stocks&active=true&order=asc&limit={LIMIT}&sort=ticker&apiKey={API_Key}'
    response=requests.get(url)
    data=response.json()
    for ticker in data['results']:
        ticker['ds']=DS
        tickers.append(ticker)
    while 'next_url' in data :
    #time.sleep(12) ## Wait 12 seconds to respect rate limit (adjust as needed)
        response= requests.get(data['next_url'] + f"&apiKey={API_Key}" ) 
        data = response.json()
        print (f"Data Keys,{data.keys()}")
        print (f"Data: {data}")
        if 'results' in data and data.get('status')=='OK':
            print (f"Data data,{data['results']}")
            for ticker in data['results']:
                ticker['ds']=DS
                tickers.append(ticker) 
    

    print(f"length tickers :{len(tickers)}")
    print(f"Tickers data :{tickers}")

    
    example_ticker={'ticker': 'ZEOWW', 'name': 'Zeo Energy Corporation Warrants',
                'market': 'stocks', 'locale': 'us', 'primary_exchange': 'XNAS', 
                'type': 'WARRANT', 'active': True, 'currency_name': 'usd', 'cik': '0001865506',
                'composite_figi': 'BBG0140GM330', 
                'ds' : '2025-10-05',
                'last_updated_utc': '2025-10-05T06:05:16.273438006Z'}
    csv_fields = list(example_ticker.keys())

    with open('tickers.json', 'w') as f:
        json.dump(tickers, f)

    load_to_snowflake(tickers,csv_fields)
    print(f"Loaded {len(tickers)} rows to Snowflake.")

'''
def load_to_snowflake(tickers):
    ctx = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA
    )
    cs = ctx.cursor()
    try:
        cs.execute(f"USE WAREHOUSE {SNOWFLAKE_WAREHOUSE}")

        insert_sql = f"""
        INSERT INTO {SNOWFLAKE_TABLE} ({', '.join([f'"{field}"' for field in csv_fields])})
        VALUES ({', '.join(['%s'] * len(csv_fields))})
        """

        data_to_insert = []
        for ticker in tickers:
            row = [ticker.get(field, None) for field in csv_fields]
            data_to_insert.append(row)

        cs.executemany(insert_sql, data_to_insert)
        print(f"Inserted {len(data_to_insert)} rows into {SNOWFLAKE_TABLE}.")

    finally:
        cs.close()
        ctx.close()
'''
def load_to_snowflake(tickers,csv_fields):
    # Build connection kwargs from environment variables
    connect_kwargs = {
        'user': os.getenv('SNOWFLAKE_USER'),
        'password': os.getenv('SNOWFLAKE_PASSWORD'),
    }
    account = os.getenv('SNOWFLAKE_ACCOUNT')
    if account:
        connect_kwargs['account'] = account
    warehouse = os.getenv('SNOWFLAKE_WAREHOUSE')
    database = os.getenv('SNOWFLAKE_DATABASE')
    schema = os.getenv('SNOWFLAKE_SCHEMA')
    role = os.getenv('SNOWFLAKE_ROLE')

    connect_kwargs['warehouse'] = warehouse
    if database:
        connect_kwargs['database'] = database
    if schema:
        connect_kwargs['schema'] = schema
    if role:
        connect_kwargs['role'] = role

    print(connect_kwargs)
    conn = snowflake.connector.connect(
        user=connect_kwargs['user'],
        password=connect_kwargs['password'],
        account=connect_kwargs['account'],
        warehouse=connect_kwargs['warehouse'],
        database=connect_kwargs['database'],
        schema=connect_kwargs['schema'],
        role=connect_kwargs['role'],
        session_parameters={
            "CLIENT_TELEMETRY_ENABLED": False,
        }
    )
    cs = conn.cursor()
    try:
        cs.execute(f"USE WAREHOUSE {connect_kwargs['warehouse']}")

        # Define typed schema based on example_ticker
        type_overrides = {
            'ticker': 'VARCHAR',
            'name': 'VARCHAR',
            'market': 'VARCHAR',
            'locale': 'VARCHAR',
            'primary_exchange': 'VARCHAR',
            'type': 'VARCHAR',
            'active': 'BOOLEAN',
            'currency_name': 'VARCHAR',
            'cik': 'VARCHAR',
            'composite_figi': 'VARCHAR',
            'share_class_figi': 'VARCHAR',
            'last_updated_utc': 'TIMESTAMP_NTZ',
            'ds': 'VARCHAR'
        }

        # Create table if it doesn't exist
        columns_sql_parts = []
        for col in csv_fields:
            col_type = type_overrides.get(col, 'VARCHAR')
            columns_sql_parts.append(f"{col.upper()} {col_type}")
        create_table_sql = f"CREATE TABLE IF NOT EXISTS {SNOWFLAKE_TABLE} ({', '.join(columns_sql_parts)})"
        cs.execute(create_table_sql)

        # Insert data
        insert_sql = f"""
        INSERT INTO {SNOWFLAKE_TABLE} ({', '.join(csv_fields)})
        VALUES ({', '.join(['%s'] * len(csv_fields))})
        """

        data_to_insert = []
        for ticker in tickers:
            row = [ticker.get(field, None) for field in csv_fields]
            data_to_insert.append(row)

        cs.executemany(insert_sql, data_to_insert)
        print(f"Inserted {len(data_to_insert)} rows into {SNOWFLAKE_TABLE}.")

    finally:
        cs.close()
        conn.close()
if __name__ == "__main__":
    run_stock_job()
