
from ..data.data_providers.time_series_loader import TimeSeriesProvider, YahooTimeSeriesProvider

if __name__ == "__main__":
    yf = YahooTimeSeriesProvider()
    ts = yf.get_time_series('AAPL', '2020-09-30', '2025-09-30')
    print(ts)