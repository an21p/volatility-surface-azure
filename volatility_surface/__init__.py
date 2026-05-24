import azure.functions as func
import json
import logging
import utils
import numpy as np
import pandas as pd

from datetime import datetime
from typing import Optional
from visualiser import build_surface
from volatility_surface.surface_page import render_surface_html
from volatility_surface.surface_page import compute_stats
from volatility_surface.analysis import coarse_grid, analyse_surface


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

    spot = float(df['spot'].iloc[0]) if 'spot' in df.columns and not df.empty else None
    html = render_surface_html(
        ticker=ticker,
        date_str=datetime.strftime(date, '%Y-%m-%d'),
        strikes=unique_strikes,
        tenors=expiry_periods,
        vol_surface=vol_surface,
        spot=spot,
    )

    return func.HttpResponse(
        body=html,
        status_code=200,
        mimetype="text/html"
    )


analysis = func.Blueprint()


@analysis.function_name(name="SurfaceAnalysisTrigger")
@analysis.route(route="surface-analysis", auth_level=func.AuthLevel.ANONYMOUS)
def get_surface_analysis(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('surface_analysis: start')

    ticker = req.params.get('ticker', 'SPY').strip().upper()
    date_str = req.params.get(
        'date', datetime.strftime(datetime.now(), '%Y-%m-%d'))
    try:
        date = datetime.strptime(date_str, '%Y-%m-%d')
    except ValueError:
        return func.HttpResponse("Invalid date format. Please use YYYY-MM-DD.",
                                 status_code=400)

    container = utils.setup_blob_container()
    blob_name = utils.get_analysis_blob_name(ticker, date)

    # Cache hit -> return immediately, no model call.
    if container is not None and utils.blob_exists(container, blob_name):
        cached = utils.read_text_blob(container, blob_name)
        return func.HttpResponse(
            json.dumps({"analysis": cached, "cached": True}),
            mimetype="application/json")

    # Cache miss -> build the surface, derive stats + grid, ask the model.
    try:
        df = get_option_data(ticker, date)
        spot = float(df['spot'].iloc[0]) if 'spot' in df.columns and not df.empty else None
        strikes, tenors, vol_surface = build_surface(df, ticker)
        if spot is None:
            spot = float(np.mean(strikes)) if len(strikes) else 0.0
        stats = compute_stats(strikes, tenors, vol_surface, spot)
        grid = coarse_grid(strikes, tenors, vol_surface)
        text = analyse_surface(ticker, date_str, stats, grid)
    except Exception:
        logging.exception("surface_analysis: failed to build analysis")
        text = None

    if text and container is not None:
        try:
            utils.write_text_blob(container, blob_name, text)
        except Exception:
            logging.exception("surface_analysis: failed to cache analysis")

    return func.HttpResponse(
        json.dumps({"analysis": text, "cached": False}),
        mimetype="application/json")
