import azure.functions as func

from os import getenv
from logging import info, warning, error
from typing import Optional
from datetime import datetime, timezone
from DownloaderTrigger.utils import fetch, setup_blob_container, blob_exists, is_third_friday, data_frame_from_blob

import pandas as pd


def upload_filtered_options(ticker: str, container_client, raw_options: Optional[pd.DataFrame], blob_name: str) -> None:
    """
    Filters the raw options DataFrame and uploads the filtered data to the blob container.
    """
    if raw_options is None or raw_options.empty:
        warning(f"No raw options data to filter for {ticker}.")
        return

    df = raw_options

    # Identify all third‐Friday expiries in the JSON
    distinct_expiries = df["expiry"].drop_duplicates()

    third_fridays = sorted(
        {dt for dt in distinct_expiries if is_third_friday(dt)})

    # 4. Pick the NEXT third‐Friday (>= today)
    today = pd.Timestamp.today().normalize()
    future_std = [dt for dt in third_fridays if dt >= today]
    if not future_std:
        error("No upcoming standard expiry found.")
        return

    # 5. Filter the DataFrame to only that expiration
    df_next_monthly = df[df["expiry"].isin(future_std)]
    df_next_monthly = df_next_monthly[df_next_monthly["type"] == "C"]
    df_next_monthly["atm_diff"] = (
        df_next_monthly["strike"] - df_next_monthly['spot']).abs()

    # 6. Pick the 10 strikes nearest ATM
    df_near_atm = df_next_monthly.sort_values("atm_diff").groupby(
        "expiry").head(10).sort_values("expiry")

    # 7. Save to CSV
    cols = ["expiry", "spot", "strike", "bid", "ask", "iv", "type"]
    df_near_atm = df_near_atm[cols]

    if len(df_near_atm) > 0:
        data = df_near_atm.to_csv(index=False).encode('utf-8')
        info(df_near_atm.head())
        container_client.upload_blob(
            name=blob_name,
            data=data,
            overwrite=True
        )
        info(f"Uploaded blob: {blob_name}")
        info(
            f"Successfully fetched and stored options data for {ticker}.")
    else:
        info('No Datasets have been updated in the last two weeks')

    return None


def download_and_upload_raw_options(ticker: str, container_client) -> Optional[pd.DataFrame]:
    """
    Downloads options data for the given ticker and uploads it to the blob container if not already present.
    """
    raw_blob_name = f"{ticker}_raw_options_{datetime.now().strftime('%Y%m%d')}.csv"

    if blob_exists(container_client, raw_blob_name):
        info(f"Blob {raw_blob_name} already exists. Skipping upload.")
        return data_frame_from_blob(container_client, raw_blob_name)

    info(f"Fetching options data for {ticker}...")
    try:
        df = fetch(ticker)
        if len(df) > 0:
            data = df.to_csv(index=False).encode('utf-8')
            info(df.head())
            container_client.upload_blob(
                name=raw_blob_name,
                data=data,
                overwrite=True
            )
            info(f"Uploaded blob: {raw_blob_name}")
            info(
                f"Successfully fetched and stored options data for {ticker}.")
        else:
            info('No Datasets have been updated in the last two weeks')
    except Exception as e:
        error(f"Error fetching options data: {e}")

    return None


def main(mytimer: func.TimerRequest) -> None:
    """
    Timer trigger function to fetch options data and write it to an output blob.
    """
    utc_timestamp = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()

    if mytimer.past_due:
        info('The timer is past due!')

    conn_str = getenv("AZURE_STORAGE_CONNECTION_STRING")
    if not conn_str:
        error("AZURE_STORAGE_CONNECTION_STRING is not set")
        return

    container_name = "downloads"
    container_client = setup_blob_container(conn_str, container_name)

    tickers = ["AAPL", "GOOGL", "MSFT", "TSLA", "AMZN", "NVDA"]
    for ticker in tickers:
        info(f"Processing ticker: {ticker}")
        filtered_blob_name = f"{ticker}_filtered_options_{datetime.now().strftime('%Y%m%d')}.csv"
        if blob_exists(container_client, filtered_blob_name):
            info(
                f"Filtered blob {filtered_blob_name} already exists. Skipping.")
            continue
        raw_options = download_and_upload_raw_options(ticker, container_client)
        upload_filtered_options(ticker, container_client,
                                raw_options, filtered_blob_name)

    info('Python timer trigger function ran at %s', utc_timestamp)
