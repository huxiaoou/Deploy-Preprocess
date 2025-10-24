import yaml
from qtools_sxzq.qdata import CDataDescriptor
from typedef import TUniverse, CCfgInstru
from typedef import CCfgProj, CCfgMajor, CCfgDbs


with open("config.yaml", "r") as f:
    _config = yaml.safe_load(f)

universe: TUniverse = {k: CCfgInstru(**v) for k, v in _config["universe"].items()}
universe_sector: dict[str, str] = {k: v.sectorL1 for k, v in universe.items()}

cfg = CCfgProj(
    path_calendar=_config["path_calendar"],
    codes=list(universe),
    major=CCfgMajor(**_config["major"]),
    dbs=CCfgDbs(**_config["dbs"]),
)

"""
-------------------
--- public data ---
-------------------
"""

data_desc_pv = CDataDescriptor(codes=[], **_config["src_tables"]["pv"])
data_desc_funda = CDataDescriptor(codes=cfg.codes, **_config["src_tables"]["funda"])

"""
-----------------
--- user data ---
-----------------
"""

data_desc_preprocess = CDataDescriptor(db_name=cfg.dbs.user, codes=cfg.codes, **_config["output_tables"]["preprocess"])
data_desc_dominant = CDataDescriptor(
    db_name=cfg.dbs.user,
    codes=[code.replace("9999", "").split("_")[0] for code in cfg.codes],
    **_config["output_tables"]["dominant"],
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
    print(sep("data_pv"))
    print(data_desc_pv)
    print(sep("data_funda"))
    print(data_desc_funda)
    print(sep("preprocess"))
    print(data_desc_preprocess)
    print(sep("dominant"))
    print(data_desc_dominant)
