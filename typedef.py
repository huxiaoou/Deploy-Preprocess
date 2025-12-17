from dataclasses import dataclass
from typing import Literal


@dataclass(frozen=True)
class CCfgInstru:
    sectorL0: Literal["C", "E"]  # C = commodity, E = Equity
    sectorL1: Literal["AUG", "MTL", "BLK", "OIL", "CHM", "AGR"]


@dataclass(frozen=True)
class CCfgMajor:
    vol_alpha: float


@dataclass(frozen=True)
class CCfgAvlb:
    window: int
    threshold: float
    keep: int

    @property
    def lag(self) -> int:
        return max(self.window, self.keep) * 2


TInstruName = str
TUniverse = dict[TInstruName, CCfgInstru]


@dataclass(frozen=True)
class CCfgDbs:
    public: str
    basic: str
    user: str


@dataclass(frozen=True)
class CCfgProj:
    path_calendar: str
    codes: list[str]
    major: CCfgMajor
    avlb: CCfgAvlb
    dbs: CCfgDbs
