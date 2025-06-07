
from io import StringIO
from azure.storage.blob import BlobServiceClient

from logging import debug
from requests import get
from calendar import monthcalendar

import pandas as pd


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


def setup_blob_container(conn_str: str, container_name: str):
    """
    Sets up the Azure Blob container client and ensures the container exists.
    Returns the container client.
    """
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
