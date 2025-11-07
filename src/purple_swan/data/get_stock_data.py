import pandas as pd

nasdaq = pd.read_csv("https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt", sep="|")
other = pd.read_csv("https://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt", sep="|").rename(columns={"ACT Symbol":"Symbol"})
stocks = pd.concat([nasdaq[['Symbol','Security Name']], other[['Symbol','Security Name']]])

url="s3://pswn-test/markat_data/import/stocks.csv"
#url="s3://pswn-funds/markat-data/import/stocks.csv"

stocks.to_csv(url,index=False,header=True)
url="s3://pswn-test/markat_data/assets/nasdaq.csv"
nasdaq.to_csv(url,index=False,header=True)
url="s3://pswn-test/markat_data/import/other.csv"
other.to_csv(url,index=False,header=True)

stocks.set_index(['Symbol'], inplace=True)

