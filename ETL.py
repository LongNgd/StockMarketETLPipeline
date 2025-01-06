import logging
import yfinance as yf
import requests
import pandas as pd
from bs4 import BeautifulSoup
from pandas_gbq import to_gbq

# Configure logging
logging.basicConfig(
    filename='etl.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Function to extract stock information from Yahoo Finance
def extract_stock_info():
    logging.info("Starting to extract stock information from Yahoo Finance.")
    url = "https://finance.yahoo.com/markets/stocks/most-active/"
    try:
        response = requests.get(url)
        response.raise_for_status()
        logging.info("Successfully fetched data from Yahoo Finance.")
    except requests.exceptions.RequestException as e:
        logging.error(f"Error while making request to {url}: {e}", exc_info=True)
        return pd.DataFrame()

    soup = BeautifulSoup(response.text, 'html.parser')
    stocks = []
    row_count = 0
    error_count = 0

    for row in soup.find_all('tr', {'class': 'row false yf-paf8n5'}):
        try:
            symbol = row.find('span', {'class': 'symbol yf-1m808gl'}).text.strip()
            name = row.find('div', {'class': 'tw-pl-4 yf-h8l7j7'}).text.strip()
            stocks.append({'symbol': symbol, 'name': name})
            row_count += 1
        except AttributeError:
            error_count += 1
            logging.warning("Row parsing failed due to missing elements. Skipping row.")
            continue

    logging.info(f"Finished extracting stock information: {row_count} rows successfully parsed, {error_count} rows failed.")
    return pd.DataFrame(stocks)

# Function to download stock prices
def download_stock_prices(stock_info):
    logging.info("Starting to download stock prices.")
    stock_price = pd.DataFrame()

    for symbol in stock_info['symbol']:
        try:
            stock_data = yf.download(tickers=symbol, period="3mo")
            stock_data.reset_index(inplace=True)
            stock_data['ticker'] = symbol
            stock_data = stock_data[['ticker', 'Date', 'Open', 'High', 'Low', 'Close', 'Volume']]
            stock_data.columns = ['ticker', 'date', 'open', 'high', 'low', 'close', 'volume']
            stock_price = pd.concat([stock_price, stock_data], ignore_index=True)
        except Exception as e:
            logging.error(f"Cannot fetch data for {symbol}: {e}", exc_info=True)

    logging.info("Finished downloading stock prices.")
    return stock_price

# Function to fetch exchange rate data
def fetch_exchange_rate():
    logging.info("Starting to fetch exchange rate data.")
    currency_pair = "USDVND=X"
    try:
        rate_data = yf.download(currency_pair, period="3mo", interval="1d")
        rate_data.reset_index(inplace=True)
        rate_data = rate_data[['Date', 'Close']]
        rate_data.columns = ['date', 'rate_USDVND']
        logging.info("Successfully fetched exchange rate data.")
        return rate_data
    except Exception as e:
        logging.error(f"Error fetching exchange rate data: {e}", exc_info=True)
        return pd.DataFrame()

# Function to calculate technical indicators
def calculate_indicators(stock_price):
    logging.info("Starting to calculate technical indicators.")
    results = []

    try:
        for ticker in stock_price['ticker'].unique():
            ticker_data = stock_price[stock_price['ticker'] == ticker].copy()
            indicator_data = pd.DataFrame()
            indicator_data['ticker'] = ticker_data['ticker']
            indicator_data['date'] = ticker_data['date']
            indicator_data['RSI'] = calculate_rsi(ticker_data)
            indicator_data['SMA_20'] = calculate_sma(ticker_data)
            indicator_data['EMA_20'] = calculate_ema(ticker_data)
            sma, upper_band, lower_band = calculate_bollinger_bands(ticker_data)
            indicator_data['Bollinger_SMA'] = sma
            indicator_data['Bollinger_Upper'] = upper_band
            indicator_data['Bollinger_Lower'] = lower_band
            results.append(indicator_data)

        logging.info("Successfully calculated technical indicators.")
        return pd.concat(results, ignore_index=True)
    except Exception as e:
        logging.error(f"Error calculating technical indicators: {e}", exc_info=True)
        return pd.DataFrame()

# Function to upload data to BigQuery
def upload_to_bigquery(df, table_id, project_id='dinhlong', dataset_id='stock_dataset', if_exists='replace'):
    try:
        logging.info(f"Uploading data to BigQuery table {dataset_id}.{table_id}.")
        to_gbq(df, f'{dataset_id}.{table_id}', project_id=project_id, if_exists=if_exists)
        logging.info(f"Successfully uploaded data to {dataset_id}.{table_id}.")
    except Exception as e:
        logging.error(f"Error uploading to BigQuery table {dataset_id}.{table_id}: {e}", exc_info=True)

# RSI Calculation
def calculate_rsi(data, column='close', window=14):
    delta = data[column].diff(1)
    gain = delta.where(delta > 0, 0).rolling(window=window).mean()
    loss = -delta.where(delta < 0, 0).rolling(window=window).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))

# SMA Calculation
def calculate_sma(data, column='close', window=20):
    return data[column].rolling(window=window).mean()

# EMA Calculation
def calculate_ema(data, column='close', span=20):
    return data[column].ewm(span=span, adjust=False).mean()

# Bollinger Bands Calculation
def calculate_bollinger_bands(data, column='close', window=20):
    sma = calculate_sma(data, column, window)
    std = data[column].rolling(window=window).std()
    upper_band = sma + (2 * std)
    lower_band = sma - (2 * std)
    return sma, upper_band, lower_band

# Main function to orchestrate the process
def main():
    logging.info("ETL process started.")

    try:
        # Step 1: Extract data
        stock_info = extract_stock_info()
        stock_price = download_stock_prices(stock_info)
        rate_data = fetch_exchange_rate()

        # Step 2: Transform data
        stock_indicator = calculate_indicators(stock_price)

        # Step 3: Load data
        upload_to_bigquery(stock_info, 'stock_info')
        upload_to_bigquery(stock_price, 'stock_price')
        upload_to_bigquery(stock_indicator, 'stock_indicator')
        upload_to_bigquery(rate_data, 'rate_data')

        logging.info("ETL process completed successfully.")
    except Exception as e:
        logging.error(f"ETL process failed: {e}", exc_info=True)

if __name__ == "__main__":
    main()
