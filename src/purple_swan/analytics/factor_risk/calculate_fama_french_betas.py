from pandas import read_csv, DataFrame,read_parquet
import datetime as dt
import numpy as np


import pandas as pd
import numpy as np
import datetime as dt

import yfinance as yf
from pandas_datareader import data as web
import statsmodels.api as sm

df_ff = read_csv("s3://pswn-test/markat_data/factors/ff/ff_returns.csv")
df_ts = read_parquet("s3://pswn-test/all_time_series.parquet")

tickers = df_ts.columns
UNIVERSE = tickers
MARKET_INDEX = "^GSPC"   # S&P 500 as market proxy

START_DATE = "2023-01-01"
END_DATE = dt.date.today().strftime("%Y-%m-%d")

FREQ = "D"  # 'D' for daily; Fama-French library usually has daily & monthly variants

def get_price_data(tickers, start, end):
    data = yf.download(tickers, start=start, end=end, auto_adjust=True, progress=False)
    # yfinance returns multi-index columns: ('Adj Close', 'AAPL'), etc.
    prices = data["Close"]
    return prices

prices_stocks = df_ts
prices_mkt = get_price_data([MARKET_INDEX], START_DATE, END_DATE)[MARKET_INDEX]
dates = prices_mkt.index
dates_int = [d.strftime("%Y%m%d") for d in dates]
prices_mkt.index = dates_int
rets_stocks = np.log(df_ts / df_ts.shift(1)).dropna()
rets_mkt = np.log(prices_mkt / prices_mkt.shift(1)).dropna()

# Align dates
common_idx = rets_stocks.index.intersection(rets_mkt.index)
rets_stocks = rets_stocks.loc[common_idx]
rets_mkt = rets_mkt.loc[common_idx]

mkt_cap_now = read_csv("s3://pswn-test/markat_data/import/mkt_cap/all_mktcaps.csv")
mkt_cap_now['log_mkt_cap'] = mkt_cap_now['mkt_cap'].apply(lambda x: np.log(x))
mkt_cap_now.set_index('ticker',inplace=True)
size_exposures = pd.DataFrame(index=rets_stocks.index, columns=rets_stocks.columns)
for t in size_exposures.columns:
    if t in mkt_cap_now.index:
        lm =  mkt_cap_now.loc[t,'log_mkt_cap']
        size_exposures[t] = lm
        print(f"{t} = {lm}")

size_exposures.fillna(0, inplace=True)
print(size_exposures)

def compute_momentum(prices, lookback_12m=252, skip_1m=21):
    """
    12-month minus 1-month momentum: R(t-252 to t-21).
    """
    return np.log(prices.shift(skip_1m) / prices.shift(lookback_12m + skip_1m))

mom12m = compute_momentum(prices_stocks)
mom12m = mom12m.reindex(rets_stocks.index)
print(mom12m)


def compute_realized_vol(returns, window=60):
    return returns.rolling(window).std() * np.sqrt(252)

vol60 = compute_realized_vol(rets_stocks)
print(vol60)


def rolling_beta(asset_returns, market_returns, window=252):
    """
    For each stock, regress asset on market over a rolling window.
    """
    betas = pd.DataFrame(index=asset_returns.index, columns=asset_returns.columns, dtype=float)

    for t in range(window, len(asset_returns)):
        window_slice = slice(t - window, t)
        r_m = market_returns.iloc[window_slice].values
        X = sm.add_constant(r_m)
        for col in asset_returns.columns:
            y = asset_returns[col].iloc[window_slice].values
            if np.isfinite(y).sum() < window * 0.8:
                continue
            try:
                model = sm.OLS(y, X).fit()
                betas.iloc[t, betas.columns.get_loc(col)] = model.params[1]
            except Exception:
                continue

    return betas


beta_252 = rolling_beta(rets_stocks, rets_mkt).dropna()
print(beta_252)


style_exposures = {
    "SIZE": size_exposures,
    "MOM12M": mom12m,
    "VOL60": vol60,
    "BETA": beta_252,
}

# Stack into a long DataFrame: index=(date, asset), columns=factors
def stack_exposures(style_exposures_dict):
    dfs = []
    for fname, df in style_exposures_dict.items():
        long_df = df.stack().rename(fname)
        dfs.append(long_df)
    expos_long = pd.concat(dfs, axis=1)
    expos_long.index.names = ["date", "asset"]
    return expos_long

print("===================================================")
expos_long = stack_exposures(style_exposures)
print(expos_long.head())

rets_long = rets_stocks.stack().rename("ret")
rets_long.index.names = ["date", "asset"]

# Merge
data_cs = pd.concat([rets_long, expos_long], axis=1).dropna()
print(data_cs.head())
data_cs.reset_index(inplace=True)
for d, df in data_cs.groupby('date'):
    print(f"Saving factor data for {d}")
    url = f's3://pswn-test/markat_data/factors/date={d}/all_exposures.csv'
    df.to_csv(url,index=False,header=True)
    url = f's3://pswn-test/markat_data/factors/date={d}/all_exposures.parquet'
    df.to_parquet(url,index=False,compression='snappy')


def run_cross_sectional_regressions(data_cs, factor_names):
    """
    data_cs: long DataFrame with columns: ['ret'] + factor_names
             index: MultiIndex(date, asset)
    Returns:
        factor_returns: DataFrame (index=date, columns=factors)
        residuals: Series (index=date,asset)
    """
    factor_returns_list = []
    residuals_list = []

    for date, df in data_cs.groupby(level="date"):
        # Drop assets with missing exposures
        df = df.dropna(subset=["ret"] + factor_names)
        if df.shape[0] < len(factor_names) + 1:
            continue

        y = df["ret"].values
        X = df[factor_names].values
        X = sm.add_constant(X)

        model = sm.OLS(y, X).fit()
        params = model.params
        resid = model.resid

        ser = pd.Series(params[1:], index=factor_names, name=date)  # skip intercept
        factor_returns_list.append(ser)

        # store residuals
        r = pd.Series(resid, index=df.index, name="resid")
        residuals_list.append(r)

    factor_returns = pd.DataFrame(factor_returns_list).sort_index()
    residuals = pd.concat(residuals_list).sort_index()

    return factor_returns, residuals


factor_names = ["SIZE", "MOM12M", "VOL60", "BETA"]
factor_returns, residuals = run_cross_sectional_regressions(data_cs, factor_names)

print(factor_returns.head())
print(residuals.head())


factor_cov = factor_returns.cov() * 252
factor_cov.to_csv('s3://pswn-test/markat_data/factors/factor_cov.csv')

resid_df = residuals.unstack("asset")
spec_vol = resid_df.std() * np.sqrt(252)
spec_vol.name = "specific_vol"
spec_vol

