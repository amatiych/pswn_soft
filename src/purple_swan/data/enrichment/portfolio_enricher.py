from purple_swan.data.enrichment.enrichment import DataEnricher,EnrichmentContext
from purple_swan.data.models.models  import Position,Instrument,Portfolio
from typing import List
from pandas import DataFrame,concat


class PortfolioEnricher13F(DataEnricher[Portfolio]):

    def can_enrich(self,data_type: type) -> bool:
        return data_type == Portfolio

    def enrich(self, portfolios: List[Portfolio], context: EnrichmentContext) -> List[Portfolio]:
        positions = context.cache.get("positions")

        for portfolio in portfolios:
            cik = portfolio.cik
            port_positions = {p.ticker:p for p in positions if str(p.cik) == str(cik)}
            if len(port_positions) > 0:
                portfolio.positions = port_positions
        return portfolios

class PortfolioTSMatrixEnricher(DataEnricher[Portfolio]):

    def can_enrich(self, data_type: type) -> bool:
        return data_type == Portfolio

    def enrich(self, portfolios: List[Portfolio], context: EnrichmentContext) -> List[Portfolio]:
        ts_data = context.cache.get("ts_matrix", [])[0].data
        ts_data.reset_index().set_index("date", inplace=True)
        tickers = ts_data.columns

        dfs = []
        for port in portfolios:
            for ticker,position in port.positions.items():
                if ticker not in tickers:
                    df =  DataFrame(ts_data.index)
                    df[ticker] = 0
                    df.set_index("date", inplace=True)
                else:
                    df = ts_data[ticker].to_frame()
                dfs.append(df)
            if len(dfs) > 0:
                port.ts_matrix = concat(dfs,axis=1)
        return portfolios


class PortfolioFactorMatrixEnricher(DataEnricher[Portfolio]):

    def can_enrich(self, data_type: type) -> bool:
        return data_type == Portfolio

    def enrich(self, portfolios: List[Portfolio], context: EnrichmentContext) -> List[Portfolio]:

        factor_model = context.cache.get("factor_models", [])[0].data

        for port in portfolios:
            tickers = port.positions.keys()
            df = DataFrame(index=tickers,columns=factor_model.columns)
            df.fillna(0.0,inplace=True)
            common_index = df.index.intersection(factor_model.index)
            df.loc[common_index,:] = factor_model.loc[common_index,:]
            port.factor_matrix = df
        return portfolios