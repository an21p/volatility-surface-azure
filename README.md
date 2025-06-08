# Volatility Surface Functions

This project provides an Azure Functions-based backend for fetching, filtering, and serving options data from the CBOE JSON endpoint, with the goal of supporting volatility surface construction.

## Features

- **Scheduled Data Download:** Uses an Azure Functions timer trigger to periodically fetch and store options data for selected tickers from the CBOE API.
- **Data Filtering:** Processes raw options data to extract near-the-money call options for the next standard (third Friday) expiry.
- **HTTP API:** Exposes an HTTP-triggered endpoint to retrieve filtered options data for a given ticker and date, suitable for building volatility surfaces.
- **Azure Blob Storage:** Stores both raw and filtered options data in Azure Blob Storage for efficient access and persistence.

## Usage

- Deploy to Azure Functions.
- Configure your Azure Storage connection string in environment variables.
- Use the `/api/volatility-surface?ticker=TSLA` endpoint to retrieve filtered options data.

## Structure

- `function_app.py`: Registers all Azure Functions.
- `downloader_trigger/`: Timer-triggered function for scheduled data download.
- `option_data/`: HTTP-triggered function for serving filtered options data.
- `utils/`: Shared utilities for data fetching, filtering, and storage.

## Requirements

- Python 3.12+
- Azure Functions Core Tools
- Azure Storage account

See [requirements.txt](requirements.txt) for Python dependencies.


## Run locally

```bash
func start
```
