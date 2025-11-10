import pandas as pd
import numpy as np
import datetime as dt

import yfinance as yf
from pandas_datareader import data as web
import statsmodels.api as sm


url = f"s3://pswn-test/markat_data/factors/ff/ff_returns.csv"

ff = web.DataReader("F-F_Research_Data_5_Factors_2x3_daily", "famafrench")
ff_factors = ff[0] / 100.0
ff_factors.to_csv(url,index=True,header=True)
print(url)
print(ff_factors.head())