from purple_swan.data.data_factory import DataFactory
import purple_swan.data.models.s3_portfolio_data_loader as pl
import purple_swan.data.models.s3_instruments_data_loader as sl
from purple_swan.data.data_providers.time_series_loader import TimeSeriesProvider, YahooTimeSeriesProvider

if __name__ == "__main__":
    factory = DataFactory()
    factory.register(sl.S3InstrumentsDataLoader())
    factory.register(pl.S3PortfolioDataLoaderParquet)

    yf = YahooTimeSeriesProvider()
    ts = yf.get_time_series('AAPL', '2020-09-30', '2025-09-30')
    print(ts)