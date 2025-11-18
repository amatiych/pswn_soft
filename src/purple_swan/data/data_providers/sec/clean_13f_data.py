import pandas as pd
import ast
from purple_swan.core.aws_utils import list_s3_files
import json
from pandas import read_csv, read_pickle
bucket = "pswn-test"

files = list_s3_files(bucket,prefix="market_data/sec/quarter=2025Q3")

csv_files = [f for f in  files if "csv" in f]

def get_shares(x):
    d = ast.literal_eval(x)
    return d['sshPrnamt']

for f in csv_files:
    print(f)
    url = f"s3://{bucket}/{f}"
    df = pd.read_csv(url)
    if 'shrsOrPrnAmt' in df:
        df['shares'] = df['shrsOrPrnAmt'].apply(lambda x: get_shares(x))
        df['price'] = df['value']/df['shares']
        tot_value = df['value'].sum()
        df['weight'] = df['value']/tot_value
    if 'weeight' in df.columns:
        df.rename(columns={'weeight': 'weight'}, inplace=True)
    df = df[['ticker','shares','price','value','weight']]
    df.to_csv(url)
    url = url.replace("csv","parquet")
    df.to_parquet(url,compression='snappy')
