import numpy as np
import pandas as pd
from transmatrix import SignalMatrix
from transmatrix.strategy import SignalStrategy
from transmatrix.data_api import create_factor_table
from qtools_sxzq.qdata import CDataDescriptor
from qtools_sxzq.qwidgets import SFY
from typedef import CCfgCss
from solutions.misc import weighted_volatility


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

    def init(self):
        self.add_clock(milestones="15:00:00")
        self.subscribe_data("pv", self.data_desc_pv.to_args())
        self.subscribe_data("avlb", self.data_desc_avlb.to_args())
        self.create_factor_table(["val"])

    def on_clock(self):
        avlb = self.avlb.get_window_df("avlb", self.cfg_css.vma_win)[self.data_desc_avlb.codes]
        amt = self.pv.get_window_df("amt_major", self.cfg_css.vma_win)[self.data_desc_avlb.codes]
        ret = self.pv.get_window_df("pre_cls_ret_major", self.cfg_css.vma_win)[self.data_desc_avlb.codes]
        size_avlb, size_amt, size_ret = len(avlb), len(amt), len(ret)
        if any(
            [
                size_avlb < self.cfg_css.vma_win,
                size_amt < self.cfg_css.vma_win,
                size_ret < self.cfg_css.vma_win,
            ]
        ):
            print(f"[{SFY('WRN')}] {self.time} size of avlb, amt, ret does not match {SFY(self.cfg_css.vma_win)}")
            print(f"---{SFY('avlb')}---")
            print(avlb)
            print(f"---{SFY('amt')}---")
            print(amt)
            print(f"---{SFY('ret')}---")
            print(ret)
        x, wgt = ret[avlb > 0], amt[avlb > 0]
        volatility = weighted_volatility(x=x, wgt=wgt)
        vma = np.mean(volatility)
        tot_wgt = self.cfg_css.vma_wgt if vma >= self.cfg_css.vma_threshold else 1.0
        s = pd.Series({"VOL": volatility.iloc[-1], "VMA": vma, "TOTWGT": tot_wgt})[self.codes]
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
