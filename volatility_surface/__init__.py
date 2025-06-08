import azure.functions as func
import logging
import utils
import pandas as pd

from datetime import datetime
from typing import Optional
from visualiser import build_surface
import numpy as np
import plotly.graph_objs as go


option_data = func.Blueprint()


def get_option_data(ticker: str, date: datetime) -> Optional[pd.DataFrame]:
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
        df['spot'] = df['strike'].astype(float)
        df['strike'] = df['strike'].astype(float)
        logging.info(
            f"option_data: Data for {ticker} on {date} {df.head()}")
        return df
    else:
        logging.info(
            f"option_data: Downloading missung data for {ticker} on {date}")
        raw_options = utils.download_and_upload_raw_options(
            ticker, container_client)
        filtered_options = utils.upload_filtered_options(ticker, container_client,
                                                         raw_options, filtered_blob_name)
        if filtered_options is None or filtered_options.empty:
            logging.error(
                f"option_data: No filtered options data for {ticker} on {date}")
            raise Exception(f"No filtered options data for {ticker} on {date}")

        logging.info(
            f"option_data: Data for {ticker} on {date} {filtered_options.head()}")
        return filtered_options


@option_data.function_name(name="OptionDataTrigger")
@option_data.route(route="option-data", auth_level=func.AuthLevel.ANONYMOUS)
def get_data(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('option_data: start')

    ticker = req.params.get('ticker', 'SPY').strip().upper()
    date = req.params.get(
        'date', datetime.strftime(datetime.now(), '%Y-%m-%d'))
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
        df = get_option_data(ticker, date)
    except Exception:
        logging.error(
            f"option_data: Error building surface for {ticker} on {date}")
        return func.HttpResponse("Something went wrong while building the volatility surface.", status_code=500)

    if df is None or df.empty:
        logging.error(f"option_data: No data available for {ticker} on {date}")
        return func.HttpResponse("No data available for the specified ticker and date.", status_code=404)
    return func.HttpResponse(df.to_json(orient="records"), mimetype="application/json")


renderer = func.Blueprint()

@renderer.function_name(name="RendererTrigger")
@renderer.route(route="volatility-surface", auth_level=func.AuthLevel.ANONYMOUS)
def render(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('render: start')

    ticker = req.params.get('ticker', 'SPY').strip().upper()
    date = req.params.get(
        'date', datetime.strftime(datetime.now(), '%Y-%m-%d'))
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
        df = get_option_data(ticker, date)
    except Exception:
        logging.error(
            f"render: Error building surface for {ticker} on {date}")
        return func.HttpResponse("Something went wrong while building the volatility surface.", status_code=500)

    if df is None or df.empty:
        logging.error(f"render: No data available for {ticker} on {date}")
        return func.HttpResponse("No data available for the specified ticker and date.", status_code=404)

    unique_strikes, expiry_periods, vol_surface = build_surface(df, ticker)

    X, Y = np.meshgrid(unique_strikes, expiry_periods)
    fig = go.Figure(data=[go.Surface(x=X, y=Y, z=vol_surface, colorscale='Viridis')])
    fig.update_layout(
        scene=dict(
            xaxis_title='Strike',
            yaxis_title='Expiry (years)',
            zaxis_title='Implied Volatility',
            camera=dict(
                eye=dict(x=1.575, y=-1.564, z=0.410),
                up=dict(x=0, y=0, z=10) 
            )
        ),
        title=f'{ticker} Implied Volatility Surface'
    )
    html = fig.to_html(include_plotlyjs='cdn')

    return func.HttpResponse(
        body=html,
        status_code=200,
        mimetype="text/html"
    )
