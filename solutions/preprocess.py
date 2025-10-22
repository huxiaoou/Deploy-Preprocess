import pandas as pd
from qtools_sxzq.qdata import CDataDescriptor
from qtools_sxzq.qcalendar import CCalendar
from qtools_sxzq.qdataviewer import fetch
from tqdm import tqdm


def convert_time(date_8: str, tm: str = "15:00:00") -> str:
    return f"{date_8[0:4]}-{date_8[4:6]}-{date_8[6:8]} {tm}"


class CDataPVLoader:
    def __init__(self, data_desc_pv: CDataDescriptor):
        self.data_desc_pv = data_desc_pv

    def get_names_from_fields(self) -> list[str]:
        header = ["datetime", "code", "varity"]
        values = [f"`{z}`" if z in ["open", "close"] else z for z in self.data_desc_pv.fields]
        return header + values

    def load_src_data(self, bgn: str, end: str) -> pd.DataFrame:
        bgn_tm, end_tm = convert_time(bgn), convert_time(end)
        conds = f"datetime >= '{bgn_tm}' and datetime <= '{end_tm}'"
        names = self.get_names_from_fields()
        data = fetch(
            lib=self.data_desc_pv.db_name,
            table=self.data_desc_pv.table_name,
            names=names,
            conds=conds,
        ).sort_values(["datetime", "code"], ascending=True)
        return data


def get_code_data(src_data: pd.DataFrame, code: str) -> pd.DataFrame:
    varity = code.replace("9999", "")
    return src_data.query(f"varity == '{varity}'")


def process_by_code(
    code: str,
    code_data: pd.DataFrame,
    bgn: str,
    end: str,
    calendar: CCalendar,
    data_desc_pv: CDataDescriptor,
):
    print(code_data)
    return


def main_preprocess(
    codes: list[str],
    bgn: str,
    end: str,
    calendar: CCalendar,
    data_desc_pv: CDataDescriptor,
):
    data_pv_loader = CDataPVLoader(data_desc_pv=data_desc_pv)
    bgn_buffer = calendar.get_next_date(bgn, shift=-1)
    src_data = data_pv_loader.load_src_data(bgn=bgn_buffer, end=end)
    print(src_data)
    
    for code in tqdm(codes, desc="Preprocess by code"):
        code_data = get_code_data(src_data=src_data, code=code)
        process_by_code(
            code=code,
            code_data=code_data,
            bgn=bgn,
            end=end,
            calendar=calendar,
            data_desc_pv=data_desc_pv,
        )
    return
