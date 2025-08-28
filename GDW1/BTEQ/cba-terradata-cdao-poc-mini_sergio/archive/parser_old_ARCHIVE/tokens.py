from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, auto
from typing import List, Optional


class ControlType(Enum):
    IF_ERRORCODE = auto()
    GOTO = auto()
    LABEL = auto()
    RUN = auto()
    LOGON = auto()
    LOGOFF = auto()
    IMPORT = auto()
    EXPORT = auto()
    OS_CMD = auto()
    COLLECT_STATS = auto()
    CALL_SP = auto()
    UNKNOWN = auto()


@dataclass
class ControlStatement:
    type: ControlType
    raw: str
    args: Optional[str] = None
    line_no: Optional[int] = None


@dataclass
class SqlBlock:
    sql: str
    start_line: int
    end_line: int


@dataclass
class ParserResult:
    controls: List[ControlStatement]
    sql_blocks: List[SqlBlock]
