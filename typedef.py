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


@dataclass(frozen=True)
class CCfgICov:
    win: int


@dataclass(frozen=True)
class CCfgCss:
    vma_win: int
    vma_threshold: float
    vma_wgt: float


TInstruName = str
TUniverse = dict[TInstruName, CCfgInstru]
TSectors = list[str]


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
    icov: CCfgICov
    css: CCfgCss
    dbs: CCfgDbs
