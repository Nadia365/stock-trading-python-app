import requests 
import os 
from dotenv import load_dotenv 
import time
import json
import csv
load_dotenv()


# Load API key from environment variable
API_Key = os.getenv("API_Key")
LIMIT = 1000

url=f'https://api.polygon.io/v3/reference/tickers?market=stocks&active=true&order=asc&limit={LIMIT}&sort=ticker&apiKey={API_Key}'


response = requests.get(url)
tickers=[]
data = response.json()
print(data.keys())
print(data['results'])


while 'next_url' in data :
    #time.sleep(12) ## Wait 12 seconds to respect rate limit (adjust as needed)
    response= requests.get(data['next_url'] + f"&apiKey={API_Key}" ) 
    data = response.json()
    print (f"Data Keys,{data.keys()}")
    print (f"Data: {data}")
    if 'results' in data and data.get('status')=='OK':
        print (f"Data data,{data['results']}")
        for ticker in data['results']:
            tickers.append(ticker) 

print(f"length tickers :{len(tickers)}")
print(f"Tickers data :{tickers}")

with open('tickers.json', 'w') as f:
    json.dump(tickers, f)


example_ticker={'ticker': 'ZEOWW', 'name': 'Zeo Energy Corporation Warrants',
                'market': 'stocks', 'locale': 'us', 'primary_exchange': 'XNAS', 
                'type': 'WARRANT', 'active': True, 'currency_name': 'usd', 'cik': '0001865506',
                'composite_figi': 'BBG0140GM330', 'last_updated_utc': '2025-10-05T06:05:16.273438006Z'}


csv_fields = list(example_ticker.keys())

with open('tickers.csv', 'w', newline='', encoding='utf-8') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=csv_fields)
    writer.writeheader()
    for ticker in tickers:
        # Only write fields present in example_ticker schema
        row = {field: ticker.get(field, '') for field in csv_fields}
        writer.writerow(row)

print("Tickers data saved to tickers.csv")