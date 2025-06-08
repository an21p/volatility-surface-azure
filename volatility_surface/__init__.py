import azure.functions as func
import logging
import utils
import pandas as pd
import QuantLib as ql

from datetime import datetime
from typing import Optional

volatility_surface = func.Blueprint()


def build_surface(ticker: str, date: datetime) -> Optional[pd.DataFrame]:
    """
    Placeholder function to build the volatility surface.
    This function should contain the logic to fetch and process
    the volatility data for the given ticker.
    """
    container_client = utils.setup_blob_container()
    filtered_blob_name = utils.get_filtered_blob_name(ticker, date)

    if utils.blob_exists(container_client, filtered_blob_name):
        df = utils.data_frame_from_blob(container_client, filtered_blob_name)
        df['ask'] = df['ask'].astype(float)
        df['bid'] = df['bid'].astype(float)
        logging.info(
            f"volatility_surface: Data for {ticker} on {date} {df.head()}")
        return df
    else:
        logging.info(
            f"volatility_surface: Downloading missung data for {ticker} on {date}")
        raw_options = utils.download_and_upload_raw_options(
            ticker, container_client)
        filtered_options = utils.upload_filtered_options(ticker, container_client,
                                                         raw_options, filtered_blob_name)
        if filtered_options is None or filtered_options.empty:
            logging.error(
                f"volatility_surface: No filtered options data for {ticker} on {date}")
            raise Exception(f"No filtered options data for {ticker} on {date}")

        logging.info(
            f"volatility_surface: Data for {ticker} on {date} {filtered_options.head()}")
        return filtered_options


@volatility_surface.function_name(name="VolatilitySurfaceTrigger")
@volatility_surface.route(route="volatility-surface", auth_level=func.AuthLevel.ANONYMOUS)
def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('volatility_surface: start')

    ticker = req.params.get('ticker')
    if not ticker:
        ticker = "SPY"
    ticker = ticker.strip().upper()
    logging.info(f'volatility_surface: ticker {ticker}')

    date = req.params.get('date')
    if not date:
        date = datetime.now()
    else:
        try:
            date = datetime.strptime(date, '%Y-%m-%d')
        except ValueError:
            logging.error(
                f"Invalid date format: {date}. Expected format is YYYY-MM-DD.")
            return func.HttpResponse(
                "Invalid date format. Please use YYYY-MM-DD.",
                status_code=400
            )

    try:
        surface = build_surface(ticker, date)
    except Exception:
        logging.error(
            f"volatility_surface: Error building surface for {ticker} on {date}")
        return func.HttpResponse(
            "Something went wrong while building the volatility surface.",
            status_code=500
        )

    logging.info('volatility_surface: returning')
    if surface is None or surface.empty:
        logging.error(
            f"volatility_surface: No data available for {ticker} on {date}")
        return func.HttpResponse(
            "No data available for the specified ticker and date.",
            status_code=404
        )
    json_output = surface.to_json(orient="records")
    return func.HttpResponse(json_output, mimetype="application/json")
