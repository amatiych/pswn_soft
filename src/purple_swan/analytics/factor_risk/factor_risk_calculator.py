import math
from dataclasses import dataclass

from pandas import DataFrame
from purple_swan.data.models.models import Portfolio
from typing import Dict
from numpy import array

@dataclass
class FactorRisk:
    portfolio_var : float
    portfolio_std : float
    marginal_risk : Dict[str,float]


class FactorRiskCalculator:

    def __init__(self,factor_cov:DataFrame):
        self.factor_cov_df=factor_cov
        self.C = self.factor_cov_df.values


    def calcualte_factor_risk(self,portfolio:Portfolio):
        if portfolio.factor_matrix is None:
            raise Exception('Factor matrix is None')

        df_m = portfolio.factor_matrix

        factors = df_m.columns
        w = array([portfolio.positions[ticker].weight for ticker in df_m.index])
        w = w.reshape(1,len(w))
        W  = w @ df_m.values

        V = W @ self.C @ W.T
        C = math.sqrt(V)

        mr = 2 * self.C @ W.T
        risk_contr = (mr * W)[0]
        risk_pct = risk_contr / (2 * V)
        risk_pct_dict = {f:float(r) for f,r in zip(factors,risk_pct[0])}
        return FactorRisk(V,C,risk_pct_dict)
