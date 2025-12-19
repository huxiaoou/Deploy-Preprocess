import pandas as pd
from qtools_sxzq.qdataviewer import fetch
from qtools_sxzq.qdata import CDataDescriptor, save_df_to_db
from qtools_sxzq.qcalendar import CCalendar
from solutions.misc import convert_time


def fetch_data(
    bgn: str,
    end: str,
    data_desc_preprocess: CDataDescriptor,
) -> pd.DataFrame:
    bgn_tm, end_tm = convert_time(bgn), convert_time(end)
    data = fetch(
        lib=data_desc_preprocess.db_name,
        table=data_desc_preprocess.table_name,
        names=["datetime", "code"] + ["code_major"],
        conds=f"datetime >= '{bgn_tm}' and datetime <= '{end_tm}'",
    )
    return data


def find_trade_day_bgn_tm(trade_day: str, calendar: CCalendar) -> pd.Timestamp:
    prev_day = calendar.get_next_date(trade_day.replace("-", ""), shift=-1)
    prev_tm = convert_time(prev_day, "21:00:00")
    return pd.Timestamp(prev_tm)


def reformat(major_data: pd.DataFrame, calendar: CCalendar, data_desc_dominant: CDataDescriptor) -> pd.DataFrame:
    dominant_data = major_data.rename(columns={"code_major": "dominant"})
    dominant_data["trade_day"] = dominant_data["datetime"].map(lambda z: z.strftime("%Y-%m-%d"))
    dominant_data["code"] = dominant_data["code"].map(lambda z: z.replace("9999", "").split("_")[0])
    dominant_data["datetime"] = dominant_data["trade_day"].map(lambda z: find_trade_day_bgn_tm(z, calendar))
    dominant_data = dominant_data[data_desc_dominant.fields]
    return dominant_data  # type:ignore


def main_dominant(
    span: tuple[str, str],
    data_desc_preprocess: CDataDescriptor,
    data_desc_dominant: CDataDescriptor,
    calendar: CCalendar,
):
    bgn, end = span
    major_data = fetch_data(
        bgn=bgn,
        end=end,
        data_desc_preprocess=data_desc_preprocess,
    )
    dominant_data = reformat(
        major_data=major_data,
        calendar=calendar,
        data_desc_dominant=data_desc_dominant,
    )
    save_df_to_db(
        df=dominant_data,
        db_name=data_desc_dominant.db_name,
        table_name=data_desc_dominant.table_name,
    )
    print(dominant_data)
    return
