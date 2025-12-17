from typing import Literal
from tqdm import tqdm
import pandas as pd
import numpy as np
from qtools_sxzq.qdata import CDataDescriptor, save_df_to_db
from qtools_sxzq.qcalendar import CCalendar
from qtools_sxzq.qdataviewer import fetch
from qtools_sxzq.qwidgets import SFG, SFY
from solutions.shared import convert_time
from typedef import CCfgMajor


class CDataLoader:
    def __init__(self, data_desc: CDataDescriptor):
        self.data_desc = data_desc

    def get_names_from_fields(self) -> list[str]:
        header = ["datetime", "code"]
        values = [f"`{z}`" if z in ["open", "close"] else z for z in self.data_desc.fields]
        return header + values

    def load_src_data(self, bgn: str, end: str) -> pd.DataFrame:
        bgn_tm, end_tm = convert_time(bgn), convert_time(end)
        conds = f"datetime >= '{bgn_tm}' and datetime <= '{end_tm}'"
        names = self.get_names_from_fields()
        data = fetch(
            lib=self.data_desc.db_name,
            table=self.data_desc.table_name,
            names=names,
            conds=conds,
        ).sort_values(["datetime", "code"], ascending=True)
        return data


def get_dates_header(bgn: str, end: str, calendar: CCalendar) -> pd.DataFrame:
    stp = calendar.get_next_date(end, shift=1)
    dates_header = calendar.get_dates_header(bgn, stp)
    dates_header["datetime"] = pd.to_datetime(dates_header["trade_date"].map(convert_time))
    dates_header = dates_header[["datetime"]]
    return dates_header  # type:ignore


def get_instru_md_data(src_md_data: pd.DataFrame, instru_code: str) -> pd.DataFrame:
    varity = instru_code.replace("9999", "")
    return src_md_data.query(f"varity == '{varity}'")


def get_instru_funda_data(src_funda_data: pd.DataFrame, instru_code: str) -> pd.DataFrame:
    return src_funda_data.query(f"code == '{instru_code}'")


def get_pre_price(instru_md_data: pd.DataFrame, price: Literal["open", "close", "settle"]) -> pd.DataFrame:
    """
    params: instru_md_data: a pd.DataFrame with columns = ["datetime", "code", price] at least
    params: price: must be one of ("open", "close", "settle")

    return : a pd.DataFrame with columns = ["datetime", "code", f"pre_{price}"]

    """
    pivot_data = pd.pivot_table(
        data=instru_md_data,
        index="datetime",
        columns="code",
        values=price,
        aggfunc=pd.Series.mean,
    )
    pivot_pre_data = pivot_data.sort_index(ascending=True).shift(1)
    pre_price_data = pivot_pre_data.stack().reset_index().rename(mapper={0: f"pre_{price}"}, axis=1)
    return pre_price_data


def cal_major_minor_code(
    instru_code: str,
    trade_date_instru_data: pd.DataFrame,
    trade_date: pd.Timestamp,
    pick_indicator: str,
) -> tuple[str, str]:
    sv = trade_date_instru_data[pick_indicator]  # a pd.Series: sum of oi and vol, with contract_id as index
    major_code = sv.idxmax()
    minor_sv = sv[sv.index > major_code]
    if not minor_sv.empty:
        minor_code = minor_sv.idxmax()
    else:
        minor_sv = sv[sv.index < major_code]
        if not minor_sv.empty:
            minor_code = minor_sv.idxmax()
            # always keep major_code is ahead of minor_code
            major_code, minor_code = minor_code, major_code
        else:
            minor_code = major_code
            print(f"There is only one ticker for {SFY(instru_code)} at {SFG(trade_date)}")
    return major_code, minor_code  # type:ignore


def find_major_and_minor_by_code(
    instru_code: str, instru_md_data: pd.DataFrame, cfg_major: CCfgMajor, slc_vars: list[str]
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    return: 2 pd.DataFrames with cols
            first:  ["datetime", "code"] + basic_inputs + major
            second:  ["datetime", "code"] + basic_inputs + minor

    """
    __pick_indicator = "vol_add_oi"

    def __reformat(raw_data: pd.DataFrame, reformat_vars: list[str]):
        if raw_data.empty:
            return pd.DataFrame(columns=reformat_vars)
        else:
            raw_data = raw_data.reset_index().rename(mapper={"index": "code"}, axis=1)
            return raw_data[reformat_vars]

    major_res, minor_res = [], []
    if not instru_md_data.empty:
        wgt = pd.Series({"volume": cfg_major.vol_alpha, "open_interest": 1 - cfg_major.vol_alpha})
        instru_md_data[__pick_indicator] = instru_md_data[["volume", "open_interest"]].fillna(0) @ wgt
        instru_md_data = instru_md_data.sort_values(
            by=["datetime", __pick_indicator, "code"],
            ascending=[True, False, True],
        ).set_index("code")
        for trade_date, trade_date_instru_data in instru_md_data.groupby(by="datetime"):  # type:ignore
            trade_date: pd.Timestamp
            trade_date_instru_data: pd.DataFrame
            major_code, minor_code = cal_major_minor_code(
                instru_code, trade_date_instru_data, trade_date, __pick_indicator
            )
            s_major = trade_date_instru_data.loc[major_code]
            s_minor = trade_date_instru_data.loc[minor_code]
            major_res.append(s_major)
            minor_res.append(s_minor)
    major_data, minor_data = pd.DataFrame(major_res), pd.DataFrame(minor_res)
    rft_vars = ["datetime", "code"] + slc_vars
    major_data, minor_data = __reformat(major_data, rft_vars), __reformat(minor_data, rft_vars)
    return major_data, minor_data  # type:ignore


def add_pre_price(instru_data: pd.DataFrame, pre_price_data: pd.DataFrame) -> pd.DataFrame:
    return pd.merge(left=instru_data, right=pre_price_data, on=["datetime", "code"], how="left")


def cal_return(instru_data: pd.DataFrame):
    def _cal_ret(a: float, b: float):
        return (a / b - 1) if (a >= 0) and (b > 0) else 0

    x, y = "open", "pre_open"
    instru_data["pre_opn_ret"] = instru_data[[x, y]].astype(np.float64).apply(lambda z: _cal_ret(z[x], z[y]), axis=1)
    x, y = "close", "pre_close"
    instru_data["pre_cls_ret"] = instru_data[[x, y]].astype(np.float64).apply(lambda z: _cal_ret(z[x], z[y]), axis=1)
    x, y = "settle", "pre_settle"
    instru_data["pre_stl_ret"] = instru_data[[x, y]].astype(np.float64).apply(lambda z: _cal_ret(z[x], z[y]), axis=1)
    return 0


def merge_all(
    instru_code: str,
    dates_header: pd.DataFrame,
    instru_maj_data: pd.DataFrame,
    instru_min_data: pd.DataFrame,
    instru_funda_data: pd.DataFrame,
) -> pd.DataFrame:
    keys = "datetime"
    merged_data = pd.merge(
        left=instru_maj_data,
        right=instru_min_data,
        on=keys,
        how="left",
        suffixes=("_major", "_minor"),
    )
    merged_data = merged_data.merge(right=instru_funda_data, on=keys, how="left")
    merged_data = pd.merge(left=dates_header, right=merged_data, on=keys, how="left")
    merged_data["code"] = instru_code
    return merged_data


def process_by_code(
    instru_code: str,
    instru_md_data: pd.DataFrame,
    instru_funda_data: pd.DataFrame,
    dates_header: pd.DataFrame,
    cfg_major: CCfgMajor,
    slc_vars: list[str],
    data_desc_preprocess: CDataDescriptor,
) -> pd.DataFrame:
    instru_pre_opn_data = get_pre_price(instru_md_data, price="open")
    instru_pre_cls_data = get_pre_price(instru_md_data, price="close")
    instru_pre_stl_data = get_pre_price(instru_md_data, price="settle")
    instru_maj_data, instru_min_data = find_major_and_minor_by_code(
        instru_code=instru_code,
        instru_md_data=instru_md_data,
        slc_vars=slc_vars,
        cfg_major=cfg_major,
    )
    instru_maj_data = add_pre_price(instru_maj_data, instru_pre_opn_data)
    instru_maj_data = add_pre_price(instru_maj_data, instru_pre_cls_data)
    instru_maj_data = add_pre_price(instru_maj_data, instru_pre_stl_data)
    instru_min_data = add_pre_price(instru_min_data, instru_pre_opn_data)
    instru_min_data = add_pre_price(instru_min_data, instru_pre_cls_data)
    instru_min_data = add_pre_price(instru_min_data, instru_pre_stl_data)
    cal_return(instru_maj_data)
    cal_return(instru_min_data)
    merged_data = merge_all(
        instru_code=instru_code,
        dates_header=dates_header,
        instru_maj_data=instru_maj_data,
        instru_min_data=instru_min_data,
        instru_funda_data=instru_funda_data,
    )
    merged_data = merged_data[["datetime", "code"] + data_desc_preprocess.fields]
    return merged_data  # type:ignore


def main_preprocess(
    codes: list[str],
    bgn: str,
    end: str,
    cfg_major: CCfgMajor,
    data_desc_cpv: CDataDescriptor,
    data_desc_funda: CDataDescriptor,
    data_desc_preprocess: CDataDescriptor,
    slc_vars: list[str],
    calendar: CCalendar,
):
    bgn_buffer = calendar.get_next_date(bgn, shift=-1)
    data_cpv_loader = CDataLoader(data_desc=data_desc_cpv)
    src_md_data = data_cpv_loader.load_src_data(bgn=bgn_buffer, end=end).rename(
        columns={"contractmultiplier": "multiplier"}
    )
    data_funda_loader = CDataLoader(data_desc=data_desc_funda)
    src_funda_data = data_funda_loader.load_src_data(bgn=bgn_buffer, end=end)
    dates_header = get_dates_header(bgn, end, calendar)
    dfs: list[pd.DataFrame] = []
    for instru_code in tqdm(codes, desc="Preprocess by code"):
        instru_md_data = get_instru_md_data(src_md_data=src_md_data, instru_code=instru_code)
        instru_funda_data = get_instru_funda_data(src_funda_data=src_funda_data, instru_code=instru_code)
        instru_preprocess = process_by_code(
            instru_code=instru_code,
            instru_md_data=instru_md_data,
            instru_funda_data=instru_funda_data,
            dates_header=dates_header,
            cfg_major=cfg_major,
            slc_vars=slc_vars,
            data_desc_preprocess=data_desc_preprocess,
        )
        dfs.append(instru_preprocess)
    preprocess_data = pd.concat(dfs, axis=0, ignore_index=True)
    preprocess_data = preprocess_data.sort_values(["datetime", "code"], ascending=True)
    save_df_to_db(
        df=preprocess_data,
        db_name=data_desc_preprocess.db_name,
        table_name=data_desc_preprocess.table_name,
    )
    return
