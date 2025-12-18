import numpy as np
import pandas as pd
from typing import Union
from qtools_sxzq.qplot import CPlotLines


def weighted_volatility(x: pd.Series, wgt: pd.Series = None) -> float:
    if wgt is None:
        return x.std()
    else:
        w = wgt / wgt.abs().sum()
        mu = x @ w
        x2 = (x**2) @ w
        return np.sqrt(x2 - mu**2)


def weighted_mean(x: Union[pd.Series, pd.DataFrame], wgt: pd.Series = None) -> float:
    if wgt is None:
        return x.mean()
    else:
        w = wgt / wgt.abs().sum()
        return w @ x


def plot_nav(
    nav_data: pd.DataFrame,
    xtick_count_min: int,
    ylim: tuple[float, float],
    ytick_spread: float,
    fig_name: str,
    save_dir: str,
    line_style: list = None,
    line_color: list = None,
    colormap: str = "jet",
):
    artist = CPlotLines(
        plot_data=nav_data,
        line_width=1.2,
        line_style=line_style,
        line_color=line_color,
        colormap=colormap,
    )
    artist.plot()
    artist.set_axis_x(
        xtick_count=min(xtick_count_min, len(nav_data)),
        xtick_label_size=12,
        xtick_label_rotation=90,
        xgrid_visible=True,
    )
    artist.set_axis_y(
        ylim=ylim,
        ytick_spread=ytick_spread,
        update_yticklabels=False,
        ygrid_visible=True,
    )
    artist.save(
        fig_name=fig_name,
        fig_save_dir=save_dir,
        fig_save_type="pdf",
    )
    artist.close()
    return
