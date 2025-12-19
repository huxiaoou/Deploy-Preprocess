import numpy as np
import pandas as pd
from typing import Union
from qtools_sxzq.qplot import CPlotLines


def convert_time(date_8: str, tm: str = "15:00:00") -> str:
    return f"{date_8[0:4]}-{date_8[4:6]}-{date_8[6:8]} {tm}"


def weighted_volatility(x: pd.DataFrame, wgt: pd.DataFrame = None) -> pd.Series:
    if wgt is None:
        return x.std(axis=1)
    else:
        w = wgt.div(wgt.abs().sum(axis=1), axis=0)
        mu = (x * w).sum(axis=1)
        x2 = ((x**2) * w).sum(axis=1)
        return np.sqrt(x2 - mu**2)  # type:ignore


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
