from numpy import argpartition
from pandas import read_parquet
import numpy as np
import json
from purple_swan.core.timer import timed

class VaR:
    def __init__(self,*, ci, var, k, es,idx,marginal_var=[]):
        self.ci = float(ci)
        self.var = float(var)
        self.es = float(es)
        self.var_index = int(k)
        self.tail_indexes = list(idx)
        self.marginal_var = marginal_var


    def __repr__(self):
        return self.__dict__.__repr__()

    def to_json(self):
        return json.dumps(self.__dict__)

def calc_var_core(P, cis):
    """
    P   : 1D array (T,)
    cis : 1D array (n_cis,)

    Returns:
        vars_ : (n_cis,)
        ks    : (n_cis,)
        idxs  : (n_cis, T)
    """
    T = P.shape[0]
    n_cis = cis.shape[0]

    vars_ = np.empty(n_cis, dtype=np.float64)
    ks = np.empty(n_cis, dtype=np.int64)
    idxs = np.empty((n_cis, T), dtype=np.int64)

    for i in range(n_cis):
        ci = cis[i]
        k = int((1.0 - ci) * T)
        ks[i] = k

        idx = argpartition(P, k)
        idxs[i, :] = idx
        vars_[i] = -P[idx[k]]

    return vars_, ks, idxs

class VarEngine:

    def __init__(self, df_time_series, W):
        self.df_time_series = df_time_series
        self.df_returns = self.df_time_series.pct_change(1)
        self.R = self.df_returns.values.astype(np.float64)
        self.W = np.asarray(W, dtype=np.float64)

    def calc_proforma(self):
        return self.R @ self.W
    @timed
    def calc_var(self, cis):
        P = self.R @ self.W
        cis_arr = np.asarray(cis, dtype=np.float64)

        vars_, ks, idxs = calc_var_core(P, cis_arr)

        results = []
        for i, ci in enumerate(cis):
            k = ks[i]
            idx = idxs[i, :]
            es = np.mean([P[i] for i in idx[:k]])
            var = -1 * float(vars_[i])

            #marginal calc
            P_wo = P[:, None] - self.R * self.W
            kth_vals = np.partition(P_wo, k, axis=0)[k, :]  # (N,)
            var_wo = -kth_vals  # VaR without each name
            mar_var = var - var_wo  # (N,)

            results.append(
                VaR(
                    ci = float(ci),
                    var = var,
                    k = int(idx[k]),
                    es = es,
                    idx = [int(i) for i in idx[:k]],
                    marginal_var = [float(mv) for mv in mar_var]
                )
            )
        return results


if __name__ == "__main__":
    df_ts = read_parquet("s3://pswn-test/all_time_series.parquet")
    print("have ts")
    N = len(df_ts.columns)
    weights = [1.0 / N] * N
    var_e = VarEngine(df_ts, weights)
    res = var_e.calc_var([0.95])
    print(res)
