import numpy as np
import pandas as pd
from transmatrix import SignalMatrix
from transmatrix.strategy import SignalStrategy
from transmatrix.data_api import create_factor_table
from qtools_sxzq.qdata import CDataDescriptor
from typedef import CCfgCss
from solutions.math_tools import weighted_volatility


class CCrossSectionStats(SignalStrategy):
    def __init__(
        self,
        cfg_css: CCfgCss,
        data_desc_pv: CDataDescriptor,
        data_desc_avlb: CDataDescriptor,
    ):
        self.cfg_css: CCfgCss
        super().__init__(cfg_css, data_desc_pv, data_desc_avlb)
        self.css: list[dict] = []
        self.last_volatility: list[float] = []

    def init(self):
        self.add_clock(milestones="15:00:00")
        self.subscribe_data("pv", self.data_desc_pv.to_args())
        self.subscribe_data("avlb", self.data_desc_avlb.to_args())
        self.create_factor_table(["val"])

    def on_clock(self):
        avlb = self.avlb.get_dict("avlb")
        amt = self.pv.get_dict("amt_major")
        ret = self.pv.get_dict("pre_cls_ret_major")
        mkt_data = pd.DataFrame(
            {
                "avlb": avlb,
                "amt": amt,
                "ret": ret,
            }
        ).fillna(0)
        selected_data = mkt_data.query("avlb > 0")
        volatility = weighted_volatility(x=selected_data["ret"], wgt=selected_data["amt"])
        if len(self.last_volatility) >= self.cfg_css.vma_win:
            self.last_volatility.pop(0)
        self.last_volatility.append(volatility)
        vma = np.mean(self.last_volatility)
        tot_wgt = self.cfg_css.vma_wgt if vma >= self.cfg_css.vma_threshold else 1.0
        s = pd.Series({"VOL":volatility, "VMA":vma, "TOTWGT":tot_wgt})[self.codes]
        self.update_factor("val", s)


def main_process_css(
    span: tuple[str, str],
    cfg_css: CCfgCss,
    data_desc_pv: CDataDescriptor,
    data_desc_avlb: CDataDescriptor,
    data_desc_css: CDataDescriptor,
):
    cfg = {
        "span": span,
        "codes": data_desc_css.codes,
        "cache_data": False,
        "progress_bar": True,
    }

    # --- run
    mat = SignalMatrix(cfg)
    css = CCrossSectionStats(
        cfg_css=cfg_css,
        data_desc_pv=data_desc_pv,
        data_desc_avlb=data_desc_avlb,
    )
    css.set_name("css")
    mat.add_component(css)
    mat.init()
    mat.run()

    # --- save
    dst_path = f"{data_desc_css.db_name}.{data_desc_css.table_name}"
    create_factor_table(dst_path)
    css.save_factors(dst_path)
    return 0
