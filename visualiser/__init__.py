import argparse
import requests
import pandas as pd
import numpy as np
import QuantLib as ql
from datetime import date
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D  # Ensure 3D plotting is enabled

def implied_vol_mid(option, bid, ask, bsm_process, tol=1e-6, max_eval=100, vol_min=1e-4, vol_max=5.0):
    """
    Calculate implied volatility from the mid price of bid and ask quotes for a given QuantLib option.

    Parameters:
    - option: QuantLib VanillaOption (call or put)
    - bid: float, bid price
    - ask: float, ask price
    - bsm_process: QuantLib BlackScholesMertonProcess
    - tol: float, solver tolerance
    - max_eval: int, maximum number of solver evaluations
    - vol_min: float, minimum volatility bound (e.g., 0.0001)
    - vol_max: float, maximum volatility bound (e.g., 5.0)

    Returns:
    - implied volatility as float (e.g., 0.20 for 20%), or None if solver fails
    """
    mid_price = 0.5 * (bid + ask)
    try:
        vol = option.impliedVolatility(
            mid_price, bsm_process, tol, max_eval, vol_min, vol_max)
        return vol
    except RuntimeError:
        return None


def run(ticker: str = "SPY") -> None:
    n_points = 50

    data = requests.get(f"https://volsurface.azurewebsites.net/api/option-data?ticker={ticker}")
    #data = requests.get(f"http://localhost:7071/api/option-data?ticker={ticker}")
    data = pd.DataFrame(data.json())
    data['expiry'] = pd.to_datetime(data['expiry'])
    data['spot'] = data['spot'].astype(float)
    data['strike'] = data['strike'].astype(float)
    data['ask'] = data['ask'].astype(float)
    data['bid'] = data['bid'].astype(float)



    # Choose evaluation date
    today_py = date.today()
    ql_today = ql.Date(today_py.day, today_py.month, today_py.year)
    ql.Settings.instance().evaluationDate = ql_today
    ql_calendar = ql.TARGET()
    ql_day_counter = ql.Actual365Fixed()

    # Suppose:
    spot = data["spot"].iloc[0]
    risk_free_rate = 0.03    # flat risk-free rate
    dividend_yield = 0.01    # flat dividend yield
    vol_guess = 0.20    # initial guess (20%)

    # Build yield/dividend/vol term structures
    flat_ts = ql.FlatForward(ql_today, risk_free_rate, ql_day_counter)
    div_ts = ql.FlatForward(ql_today, dividend_yield, ql_day_counter)
    vol_ts = ql.BlackConstantVol(
        ql_today, ql_calendar, vol_guess, ql_day_counter)

    # Wrap them in handles
    # Use the first spot price
    spot_handle = ql.QuoteHandle(ql.SimpleQuote(spot))
    rate_handle = ql.YieldTermStructureHandle(flat_ts)
    div_handle = ql.YieldTermStructureHandle(div_ts)
    vol_handle = ql.BlackVolTermStructureHandle(vol_ts)

    # Build the Black‐Scholes–Merton process
    bsm_process = ql.BlackScholesMertonProcess(
        spot_handle, div_handle, rate_handle, vol_handle
    )

    # ─────────────── PREPARE DATA GRID
    # Sorted expiries and strikes
    unique_expiries = sorted(data["expiry"].unique())
    unique_strikes = sorted(data["strike"].unique())

    # Convert expiries to QuantLib dates
    ql_expiries = [ql.Date(d.day, d.month, d.year) for d in unique_expiries]

    n_exp = len(unique_expiries)
    n_strk = len(unique_strikes)
    vol_matrix = np.zeros((n_exp, n_strk))

    # Fill volatility matrix (initially with NaN)
    for i, exp in enumerate(unique_expiries):
        slice_df = data[data["expiry"] == exp]
        for j, K in enumerate(unique_strikes):
            row = slice_df[slice_df["strike"] == K]
            if not row.empty:
                # Extract option parameters from the row
                option_type = ql.Option.Call if row["type"].iloc[0] == "C" else ql.Option.Put
                strike = row["strike"].iloc[0]
                expiry = row["expiry"].iloc[0]
                bid = row["bid"].iloc[0]
                ask = row["ask"].iloc[0]
                ql_expiry = ql.Date(expiry.day, expiry.month, expiry.year)
                payoff = ql.PlainVanillaPayoff(option_type, strike)
                exercise = ql.EuropeanExercise(ql_expiry)
                option = ql.VanillaOption(payoff, exercise)
                vol_matrix[i, j] = implied_vol_mid(
                    option, bid, ask, bsm_process)
            else:
                vol_matrix[i, j] = np.nan

    # Handle missing vols via forward/backward fill
    for i in range(n_exp):
        row = vol_matrix[i, :]
        if np.isnan(row).any():
            # Forward fill
            for j in range(1, n_strk):
                if np.isnan(row[j]) and not np.isnan(row[j - 1]):
                    row[j] = row[j - 1]
            # Backward fill
            for j in range(n_strk - 2, -1, -1):
                if np.isnan(row[j]) and not np.isnan(row[j + 1]):
                    row[j] = row[j + 1]
            # Fill remaining with median
            row[np.isnan(row)] = np.nanmedian(vol_matrix)
            vol_matrix[i, :] = row

    volMatrix = ql.Matrix(n_strk, n_exp)
    for i in range(n_exp):
        for j in range(n_strk):
            volMatrix[j][i] = vol_matrix[i, j]

    # ─────────────── BUILD BLACK VARIANCE SURFACE
    expiry_day_counter = ql.Actual365Fixed()

    black_var_surface = ql.BlackVarianceSurface(
        ql_today,
        ql_calendar,
        ql_expiries,
        unique_strikes,
        volMatrix,
        expiry_day_counter
    )
    black_var_handle = ql.BlackVolTermStructureHandle(black_var_surface)

    # Generate 30 evenly spaced expiry dates between ql_today and the last unique_expiry
    last_expiry = unique_expiries[-1]
    date_range = [ql_today + int(i * (ql.Date(last_expiry.day, last_expiry.month,
                                 last_expiry.year) - ql_today) / (n_points - 1)) for i in range(n_points)]
    expiry_periods = [expiry_day_counter.yearFraction(
        ql_today, d) for d in date_range]

    # Select a few expiries to visualize
    vol_surface = np.zeros((len(expiry_periods), len(unique_strikes)))
    for i, t in enumerate(expiry_periods):
        for j, k in enumerate(unique_strikes):
            vol_surface[i, j] = black_var_handle.blackVol(t, k)

    # Select a few expiries to visualize
    X, Y = np.meshgrid(unique_strikes, expiry_periods)
    fig = plt.figure(figsize=(10, 7))
    ax = fig.add_subplot(111, projection='3d')
    ax.plot_surface(X, Y, vol_surface, cmap='viridis', edgecolor='none')  # type: ignore
    ax.set_xlabel('Strike')
    ax.set_ylabel('Expiry (years)')
    ax.set_zlabel('Implied Volatility')  # type: ignore
    ax.set_title(f'{ticker} Implied Volatility Surface')
    ax.view_init(elev=20, azim=-30, roll=2) # type: ignore
    plt.show()


def main():

    parser = argparse.ArgumentParser(
        description="Fetch and filter options data for a given ticker.")
    parser.add_argument(
        "--ticker", type=str, help="Ticker symbol (e.g., SPY)", default="SPY", required=False)
    args = parser.parse_args()

    try:
        run(args.ticker)
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    main()
