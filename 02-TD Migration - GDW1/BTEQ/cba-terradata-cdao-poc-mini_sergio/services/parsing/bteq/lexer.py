from __future__ import annotations

import re
from typing import List

from .tokens import ControlStatement, ControlType, SqlBlock, ParserResult


CONTROL_PATTERNS = [
    (ControlType.IF_ERRORCODE, re.compile(r"^\s*\.IF\s+ERRORCODE.*", re.IGNORECASE)),
    (ControlType.GOTO, re.compile(r"^\s*\.GOTO\s+.+", re.IGNORECASE)),
    (ControlType.LABEL, re.compile(r"^\s*\.LABEL\s+.+", re.IGNORECASE)),
    (ControlType.RUN, re.compile(r"^\s*\.RUN\b.*", re.IGNORECASE)),
    (ControlType.LOGON, re.compile(r"^\s*\.LOGON\b.*", re.IGNORECASE)),
    (ControlType.LOGOFF, re.compile(r"^\s*\.LOGOFF\b.*", re.IGNORECASE)),
    (ControlType.IMPORT, re.compile(r"^\s*\.IMPORT\b.*", re.IGNORECASE)),
    (ControlType.EXPORT, re.compile(r"^\s*\.EXPORT\b.*", re.IGNORECASE)),
    (ControlType.OS_CMD, re.compile(r"^\s*\.OS\b.*", re.IGNORECASE)),
    (ControlType.COLLECT_STATS, re.compile(r"^\s*COLLECT\s+STATS\b.*", re.IGNORECASE)),
    (ControlType.CALL_SP, re.compile(r"^\s*CALL\s+\w+\b.*", re.IGNORECASE)),
]

SQL_START = re.compile(r"^\s*(SELECT|INSERT|UPDATE|DELETE|MERGE|WITH)\b", re.IGNORECASE)


def lex_bteq(text: str) -> ParserResult:
    controls: List[ControlStatement] = []
    sql_blocks: List[SqlBlock] = []

    lines = text.splitlines()
    current_sql: List[str] = []
    current_start: int | None = None
    in_call_statement = False
    call_start_line: int | None = None

    def flush_sql(end_line: int) -> None:
        nonlocal current_sql, current_start
        if current_sql and current_start is not None:
            sql = "\n".join(current_sql).strip()
            if sql:
                sql_blocks.append(SqlBlock(sql=sql, start_line=current_start, end_line=end_line))
        current_sql = []
        current_start = None

    def flush_call(end_line: int) -> None:
        nonlocal in_call_statement, call_start_line, current_sql
        if in_call_statement and call_start_line is not None:
            call_text = "\n".join(current_sql).strip()
            controls.append(ControlStatement(
                type=ControlType.CALL_SP, 
                raw=call_text, 
                line_no=call_start_line
            ))
        in_call_statement = False
        call_start_line = None
        current_sql = []

    for idx, line in enumerate(lines, start=1):
        stripped = line.strip()
        
        # Handle multi-line CALL statements
        if in_call_statement:
            current_sql.append(line)
            # Check if CALL statement ends (look for closing parenthesis at end of line)
            if stripped.endswith(')') or stripped.endswith(');'):
                flush_call(idx)
            continue
        
        if not stripped:
            # Blank lines: keep accumulating if in SQL block
            if current_sql:
                current_sql.append(line)
            continue

        # Check for CALL statement start (handles %%VAR%% syntax)
        if re.match(r"^\s*CALL\s+", line, re.IGNORECASE):
            flush_sql(idx - 1)
            in_call_statement = True
            call_start_line = idx
            current_sql = [line]
            # Check if single-line CALL
            if stripped.endswith(')') or stripped.endswith(');'):
                flush_call(idx)
            continue

        # Control detection (lines starting with dot or keywords like COLLECT STATS)
        matched = False
        for ctype, pattern in CONTROL_PATTERNS:
            if pattern.match(line):
                flush_sql(idx - 1)
                controls.append(ControlStatement(type=ctype, raw=line.rstrip(), line_no=idx))
                matched = True
                break
        if matched:
            continue

        # SQL start/continuation detection
        if current_sql:
            current_sql.append(line)
        elif SQL_START.match(line):
            current_sql = [line]
            current_start = idx
        else:
            # Non-SQL, non-control text; flush any ongoing SQL first
            flush_sql(idx - 1)

    # Final flush
    if in_call_statement:
        flush_call(len(lines))
    else:
        flush_sql(len(lines))

    return ParserResult(controls=controls, sql_blocks=sql_blocks)
