from transmatrix import SignalMatrix
from transmatrix.strategy import SignalStrategy
from transmatrix.data_api import create_factor_table
from qtools_sxzq.qdata import CDataDescriptor
from typedef import CCfgAvlb


class CFactorAvlb(SignalStrategy):
    def __init__(self, cfg_avlb: CCfgAvlb, data_desc_pv: CDataDescriptor):
        self.cfg_avlb: CCfgAvlb
        self.data_desc_pv: CDataDescriptor
        super().__init__(cfg_avlb, data_desc_pv)

    def init(self):
        self.add_clock(milestones="15:00:00")
        self.subscribe_data("pv", self.data_desc_pv.to_args())
        self.create_factor_table(["avlb", "amt"])

    def on_clock(self):
        amt = self.pv.get_window_df("amt_major", self.cfg_avlb.window)[self.codes]
        amt_aver = amt.mean(axis=0)
        avlb_amt = amt_aver > self.cfg_avlb.threshold

        vol = self.pv.get_window_df("volume_major", self.cfg_avlb.keep)[self.codes]
        vol_sum = (vol.fillna(0) > 0).sum(axis=0)
        avlb_vol = vol_sum >= min(self.cfg_avlb.keep * 0.95, len(vol))

        avlb_tag = avlb_amt & avlb_vol
        avlb = avlb_tag.astype(int)
        self.update_factor("avlb", avlb)
        self.update_factor("amt", amt.fillna(0).iloc[-1])


def main_process_avlb(
    span: tuple[str, str],
    cfg_avlb: CCfgAvlb,
    data_desc_pv: CDataDescriptor,
    data_desc_avlb: CDataDescriptor,
):
    cfg = {
        "span": span,
        "codes": data_desc_avlb.codes,
        "cache_data": False,
        "progress_bar": True,
    }

    # --- run
    mat = SignalMatrix(cfg)
    factor_avlb = CFactorAvlb(cfg_avlb=cfg_avlb, data_desc_pv=data_desc_pv)
    factor_avlb.set_name("factor_avlb")
    mat.add_component(factor_avlb)
    mat.init()
    mat.run()

    # --- save
    dst_path = f"{data_desc_avlb.db_name}.{data_desc_avlb.table_name}"
    create_factor_table(dst_path)
    factor_avlb.save_factors(dst_path)
    return 0
