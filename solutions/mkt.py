import numpy as np
import pandas as pd
from transmatrix import SignalMatrix
from transmatrix.strategy import SignalStrategy
from transmatrix.data_api import create_factor_table
from qtools_sxzq.qdata import CDataDescriptor


class CFactorMarket(SignalStrategy):
    def __init__(
        self,
        data_desc_pv: CDataDescriptor,
        data_desc_avlb: CDataDescriptor,
        data_desc_macro: CDataDescriptor,
    ):
        super().__init__(data_desc_pv, data_desc_avlb, data_desc_macro)
        self.market_return: list[dict] = []

    def init(self):
        self.add_clock(milestones="15:00:00")
        self.subscribe_data("pv", self.data_desc_pv.to_args())
        self.subscribe_data("avlb", self.data_desc_avlb.to_args())
        self.subscribe_data("macro", self.data_desc_macro.to_args())
        self.create_factor_table(["ret"])

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
        )
        mkt_data["rel_wgt"] = np.sqrt(mkt_data["amt"].fillna(0))
        selected_data = mkt_data.query("avlb > 0")
        wgt = selected_data["rel_wgt"] / selected_data["rel_wgt"].sum()
        m0 = selected_data["ret"] @ wgt

        close_data = self.macro.get_window_df("close", 2)
        nh0100 = close_data["MACRO_9999"].pct_change().iloc[-1]
        self.update_factor("ret", [m0, nh0100])


def main_process_mkt(
    span: tuple[str, str],
    data_desc_pv: CDataDescriptor,
    data_desc_avlb: CDataDescriptor,
    data_desc_macro: CDataDescriptor,
    data_desc_mkt: CDataDescriptor,
):
    cfg = {
        "span": span,
        "codes": data_desc_mkt.codes,
        "cache_data": False,
        "progress_bar": True,
    }

    # --- run
    mat = SignalMatrix(cfg)
    factor_market = CFactorMarket(
        data_desc_pv=data_desc_pv,
        data_desc_avlb=data_desc_avlb,
        data_desc_macro=data_desc_macro,
    )
    factor_market.set_name("factor_market")
    mat.add_component(factor_market)
    mat.init()
    mat.run()

    # --- save
    dst_path = f"{data_desc_mkt.db_name}.{data_desc_mkt.table_name}"
    create_factor_table(dst_path)
    factor_market.save_factors(dst_path)
    return 0
