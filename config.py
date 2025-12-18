import yaml
from qtools_sxzq.qdata import CDataDescriptor
from typedef import TUniverse, CCfgInstru, TSectors
from typedef import CCfgProj, CCfgMajor, CCfgAvlb, CCfgICov, CCfgCss, CCfgDbs


with open("config.yaml", "r") as f:
    _config = yaml.safe_load(f)

universe: TUniverse = {k: CCfgInstru(**v) for k, v in _config["universe"].items()}
universe_sector: dict[str, str] = {k: v.sectorL1 for k, v in universe.items()}
sectors: TSectors = sorted(list(set([v.sectorL1 for v in universe.values()])))

cfg = CCfgProj(
    path_calendar=_config["path_calendar"],
    codes=list(universe),
    major=CCfgMajor(**_config["major"]),
    avlb=CCfgAvlb(**_config["avlb"]),
    icov=CCfgICov(**_config["icov"]),
    css=CCfgCss(**_config["css"]),
    dbs=CCfgDbs(**_config["dbs"]),
)

"""
-------------------
--- public data ---
-------------------
"""

data_desc_cpv = CDataDescriptor(codes=[], **_config["src_tables"]["cpv"])
data_desc_funda = CDataDescriptor(codes=cfg.codes, **_config["src_tables"]["funda"])
data_desc_macro = CDataDescriptor(**_config["src_tables"]["macro"])

"""
-----------------
--- user data ---
-----------------
"""

data_desc_preprocess = CDataDescriptor(
    db_name=cfg.dbs.user,
    codes=cfg.codes,
    **_config["output_tables"]["preprocess"],
)
data_desc_dominant = CDataDescriptor(
    db_name=cfg.dbs.user,
    codes=[code.replace("9999", "").split("_")[0] for code in cfg.codes],
    **_config["output_tables"]["dominant"],
)
data_desc_avlb = CDataDescriptor(
    db_name=cfg.dbs.user,
    codes=cfg.codes,
    **_config["output_tables"]["avlb"],
)
data_desc_mkt = CDataDescriptor(
    db_name=cfg.dbs.user,
    **_config["output_tables"]["mkt"],
)
data_desc_icov = CDataDescriptor(
    codes=cfg.codes,
    db_name=cfg.dbs.user,
    fields=[_.lower() for _ in cfg.codes],
    **_config["output_tables"]["icov"],
)
data_desc_css = CDataDescriptor(
    db_name=cfg.dbs.user,
    **_config["output_tables"]["css"],
)
data_desc_srets = CDataDescriptor(
    codes=sectors,
    db_name=cfg.dbs.user,
    **_config["output_tables"]["srets"],
)


if __name__ == "__main__":
    sep = lambda z: f"\n{z:-^60s}"
    print(sep("universe"))
    i = 0
    for k, v in universe.items():
        print(f"{i:02d} | {k:<11s} | {v}")
        i += 1
    print(sep("cfg"))
    print(cfg)
    print(sep("data_cpv"))
    print(data_desc_cpv)
    print(sep("data_funda"))
    print(data_desc_funda)
    print(sep("preprocess"))
    print(data_desc_preprocess)
    print(sep("dominant"))
    print(data_desc_dominant)
    print(sep("avlb"))
    print(data_desc_avlb)
    print(sep("mkt"))
    print(data_desc_mkt)
    print(sep("icov"))
    print(data_desc_icov)
    print(sep("css"))
    print(data_desc_css)
    print(sep("srets"))
    print(data_desc_srets)
