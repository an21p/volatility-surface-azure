
from io import StringIO
from azure.storage.blob import BlobServiceClient

from os import getenv
from logging import debug, error, info, warning
from requests import get
from calendar import monthcalendar
from datetime import datetime
from typing import Optional

import pandas as pd


def get_filtered_blob_name(ticker: str, date: datetime) -> str:
    return f"{ticker}_filtered_options_{date.strftime('%Y%m%d')}.csv"


def get_raw_blob_name(ticker: str, date: datetime) -> str:
    return f"{ticker}_raw_options_{date.strftime('%Y%m%d')}.csv"


def is_third_friday(dt: pd.Timestamp) -> bool:
    if dt.weekday() != 4:
        return False
    # Build all Fridays in that month via calendar.monthcalendar
    year, month = dt.year, dt.month
    month_cal = monthcalendar(year, month)
    fridays = [week[4]
               # collect nonzero Fridays
               for week in month_cal if week[4] != 0]
    # third Friday is index 2
    return len(fridays) >= 3 and dt.day == fridays[2]


def parse_option_codes(df):
    """
    Given a DataFrame `df` with a column named 'option' containing strings in the format:
    {ticker}{expiry_date}{call/put}{strike}
    (e.g., "TSLA250606C00050000"), this function creates four new columns:
    - 'ticker': the underlying ticker (e.g., "TSLA")
    - 'expiry': a datetime object for the expiry date (e.g., 2025-06-06)
    - 'type': either "C" for call or "P" for put
    - 'strike': the strike price as a float (e.g., 500.0)
    """
    # Define the regex pattern
    pattern = r'^([A-Z]+)(\d{6})([CP])(\d+)$'

    # Extract named groups
    extracted = df['option'].str.extract(pattern)
    extracted.columns = ['ticker', 'expiry_str', 'type', 'strike_str']

    # Convert expiry to datetime
    extracted['expiry'] = pd.to_datetime(
        extracted['expiry_str'], format='%y%m%d')

    # Convert strike to numeric float (divide by 100)
    extracted['strike'] = extracted['strike_str'].astype(int) / 1000

    # Drop the intermediate columns and concatenate with the original DataFrame
    result = pd.concat([df,
                        extracted[['ticker', 'expiry', 'type', 'strike']]],
                       axis=1)
    return result


def fetch(ticker: str) -> pd.DataFrame:
    """
    Fetches options data for the given ticker.
    """

    url = f"https://cdn.cboe.com/api/global/delayed_quotes/options/{ticker.upper()}.json"
    resp = get(url, headers={"User-Agent": "Mozilla/5.0"})
    resp.raise_for_status()
    data = resp.json()['data']

    df = pd.DataFrame(data["options"])
    current_price = float(data["current_price"])
    df = parse_option_codes(df)
    df['spot'] = current_price

    return df


def setup_blob_container():
    """
    Sets up the Azure Blob container client and ensures the container exists.
    Returns the container client.
    """
    container_name = "downloads"
    conn_str = getenv("AZURE_STORAGE_CONNECTION_STRING")
    if not conn_str:
        error("AZURE_STORAGE_CONNECTION_STRING is not set")
        return

    blob_service_client = BlobServiceClient.from_connection_string(conn_str)
    container_client = blob_service_client.get_container_client(container_name)
    try:
        container_client.create_container()
    except Exception as e:
        debug(f"Container already exists or failed to create: {e}")
    return container_client


def blob_exists(container_client, blob_name: str) -> bool:
    """
    Checks if a blob with the given name exists in the container.
    """
    try:
        container_client.get_blob_client(blob_name).get_blob_properties()
        return True
    except Exception:
        return False


def data_frame_from_blob(container_client, blob_name: str) -> pd.DataFrame:
    blob_data = container_client.get_blob_client(
        blob_name).download_blob().readall()
    df = pd.read_csv(StringIO(blob_data.decode('utf-8')))
    df['expiry'] = pd.to_datetime(df['expiry'])
    df['strike'] = df['strike'].astype(float)
    return df


def upload_filtered_options(ticker: str,
                            container_client,
                            raw_options: Optional[pd.DataFrame],
                            blob_name: str) -> Optional[pd.DataFrame]:
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
        return None

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
        return df_near_atm
    else:
        info('No Datasets have been updated in the last two weeks')

    return None


def download_and_upload_raw_options(ticker: str, container_client) -> Optional[pd.DataFrame]:
    """
    Downloads options data for the given ticker and uploads it to the blob container if not already present.
    """
    raw_blob_name = get_raw_blob_name(ticker, datetime.now())

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
            return df
        else:
            info('No Datasets have been updated in the last two weeks')
    except Exception as e:
        error(f"Error fetching options data: {e}")

    return None
