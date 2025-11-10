import pandas as pd
from purple_swan.core.aws_utils import list_s3_files

files = list_s3_files("pswn-test","yahoo_ts")
files = sorted([f for f in files if 'parquet' in f])

dfs = []
for f in files:
    print(f)
    url = f"s3://pswn-test/{f}"
    df = pd.read_parquet(url)
    dfs.append(df)
final_df = pd.concat(dfs,axis=1)
final_df.dropna(inplace=True,axis=1)
url = "s3://pswn-test/all_time_seies.csv"
print("saving csv")
final_df.to_csv(url,index=True,header=True)
url = "s3://pswn-test/all_time_seies.parquet"
print("saving parquet")
final_df.to_parquet(url,compression="snappy")
print("Done")