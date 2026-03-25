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


def weighted_average_and_variance(values: pd.Series, weights: pd.Series) -> tuple[float, float]:
    average: float = values @ weights  # type:ignore
    variance: float = values**2 @ weights - average**2  # type:ignore
    return average, variance


def decompose_variance(df: pd.DataFrame) -> tuple[float, float, float]:
    """_summary_

    Args:
        df (pd.DataFrame): has columns "return"(float), "weight"(float), and "sector"(str)

    Returns:
        tuple[float, float, float]: (total_variance, within_sector_variance, between_sector_variance)
    """

    total_average_return, total_variance = weighted_average_and_variance(df["return"], df["weight"])
    result = []
    for sector, group in df.groupby("sector"):
        sector_weight = group["weight"].sum()
        wgt_h = group["weight"] / sector_weight
        sector_average_return, within = weighted_average_and_variance(group["return"], wgt_h)
        between = (sector_average_return - total_average_return) ** 2
        result.append(
            {
                "sector": sector,
                "sector_weight": sector_weight,
                "within": within,
                "between": between,
            }
        )
    group_result = pd.DataFrame(result)
    within_sector_variance: float = group_result["within"] @ group_result["sector_weight"]  # type:ignore
    between_sector_variance: float = group_result["between"] @ group_result["sector_weight"]  # type:ignore
    if abs(total_variance - (within_sector_variance + between_sector_variance)) > 1e-6:
        raise ValueError(
            f"Warning: Total variance {total_variance:.6f} does not equal the sum of within and between variances {within_sector_variance + between_sector_variance:.6f}"
        )
    return total_variance, within_sector_variance, between_sector_variance


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
