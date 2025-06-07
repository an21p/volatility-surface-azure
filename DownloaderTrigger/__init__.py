import azure.functions as func

import os
import logging
from datetime import datetime, timezone
from utils import fetch, setup_blob_container, blob_exists

def download_and_upload_options(ticker: str, container_client, blob_name: str):
    """
    Downloads options data for the given ticker and uploads it to the blob container if not already present.
    """
    if blob_exists(container_client, blob_name):
        logging.info(f"Blob {blob_name} already exists. Skipping upload.")
        return

    logging.info(f"Fetching options data for {ticker}...")
    try:
        df = fetch(ticker)
        if len(df) > 0:
            data = df.to_csv(index=False).encode('utf-8')
            logging.info(df.head())
            container_client.upload_blob(
                name=blob_name,
                data=data,
                overwrite=True
            )
            logging.info(f"Uploaded blob: {blob_name}")
            logging.info(f"Successfully fetched and stored options data for {ticker}.")
        else:
            logging.info('No Datasets have been updated in the last two weeks')
    except Exception as e:
        logging.error(f"Error fetching options data: {e}")

def main(mytimer: func.TimerRequest) -> None:
    """
    Timer trigger function to fetch options data and write it to an output blob.
    """
    utc_timestamp = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    conn_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    if not conn_str:
        logging.error("AZURE_STORAGE_CONNECTION_STRING is not set")
        return

    container_name = "downloads"
    container_client = setup_blob_container(conn_str, container_name)

    tickers = ["AAPL", "GOOGL", "MSFT", "TSLA", "AMZN", "NVDA"]
    for ticker in tickers:
        logging.info(f"Processing ticker: {ticker}")
        blob_name = f"{ticker}_raw_options_{datetime.now().strftime('%Y%m%d')}.csv"
        download_and_upload_options(ticker, container_client, blob_name)

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
