import numpy as np
import pandas as pd


def weighted_volatility(x: pd.Series, wgt: pd.Series = None) -> float:
    if wgt is None:
        return x.std()
    else:
        w = wgt / wgt.abs().sum()
        mu = x @ w
        x2 = (x**2) @ w
        return np.sqrt(x2 - mu**2)
