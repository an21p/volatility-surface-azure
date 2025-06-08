import azure.functions as func

from logging import info
from datetime import datetime, timezone

import pandas as pd
import utils

downloader_trigger = func.Blueprint()

@downloader_trigger.function_name(name="DownloaderTrigger")
@downloader_trigger.schedule(schedule="0 0 1 * * *",
                             arg_name="downloaderTrigger",
                             run_on_startup=False,
                             use_monitor=True)
def main(downloaderTrigger: func.TimerRequest) -> None:
    """
    Timer trigger function to fetch options data and write it to an output blob.
    """
    utc_timestamp = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()

    if downloaderTrigger.past_due:
        info('The timer is past due!')

    container_client = utils.setup_blob_container()

    tickers = ["AAPL", "GOOGL", "MSFT", "TSLA", "AMZN", "NVDA"]
    for ticker in tickers:
        info(f"Processing ticker: {ticker}")
        filtered_blob_name = utils.get_filtered_blob_name(ticker, datetime.now())
        if utils.blob_exists(container_client, filtered_blob_name):
            info(
                f"Filtered blob {filtered_blob_name} already exists. Skipping.")
            continue
        raw_options = utils.download_and_upload_raw_options(ticker, container_client)
        utils.upload_filtered_options(ticker, container_client,
                                raw_options, filtered_blob_name)

    info('Python timer trigger function ran at %s', utc_timestamp)
