# stock-trading-python-app
This uses the Polygon.io API to extract data about stocks
# Stock Trading Python App

This script fetches all active US stock tickers from the Polygon.io API and saves them to both a JSON and a CSV file. The CSV file uses a fixed schema based on an example ticker.

## Features

- Fetches all active stock tickers from Polygon.io (with pagination support)
- Saves the full ticker data to `tickers.json`
- Saves selected fields to `tickers.csv` with a consistent schema

## Requirements

- Python 3.7+
- [requests](https://pypi.org/project/requests/)
- [python-dotenv](https://pypi.org/project/python-dotenv/)

## Setup

1. **Clone the repository** (if applicable) or copy the script to your project folder.

2. **Install dependencies:**
   ```sh
   pip install requests python-dotenv
   ```

3. **Set up your `.env` file:**

   Create a `.env` file in the project directory with your Polygon.io API key:
   ```
   API_Key=your_polygon_api_key_here
   ```

## Usage

Run the script:

```sh
python script.py
```

- The script will fetch all active stock tickers and save:
  - `tickers.json`: All ticker data as returned by the API.
  - `tickers.csv`: Only selected fields, matching the schema of the provided `example_ticker`.

## Output

- **tickers.json**: List of all ticker dictionaries from Polygon.io.
- **tickers.csv**: CSV file with columns:
  - ticker
  - name
  - market
  - locale
  - primary_exchange
  - type
  - active
  - currency_name
  - cik
  - composite_figi
  - last_updated_utc

## Notes

- The script respects Polygon.io's pagination and rate limits (uncomment the `time.sleep(12)` line if you hit rate limits).
- Only tickers with the fields present in `example_ticker` will have those fields populated in the CSV; missing fields will be empty.

## License

MIT License (add your license here
