from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Dict, Set

from .bteq_lexer import lex_bteq
from .td_sql_parser import (
    transpile_to_snowflake, 
    analyze_sql_metadata, 
    get_query_complexity_metrics,
    extract_teradata_specific_features,
    validate_sql_syntax,
    optimize_sql
)
from .tokens import ParserResult, SqlBlock


@dataclass
class ParsedSql:
    original: str
    snowflake_sql: Optional[str]
    start_line: int
    end_line: int
    error: Optional[str] = None
    # Advanced SQLGlot analysis
    metadata: Optional[Dict[str, Set[str]]] = None
    complexity_metrics: Optional[Dict[str, int]] = None
    teradata_features: Optional[List[str]] = None
    syntax_validation: Optional[Dict[str, any]] = None
    optimized_sql: Optional[str] = None


@dataclass
class ParserServiceOutput:
    controls = []
    sql: List[ParsedSql] = None  # type: ignore[assignment]


class ParserService:
    """Enhanced parser orchestration service with advanced SQLGlot features.

    - Splits BTEQ into control statements and SQL blocks
    - Transpiles Teradata SQL to Snowflake SQL using SQLGlot
    - Provides advanced SQL analysis: metadata extraction, complexity metrics, optimization
    - Identifies Teradata-specific features for migration planning
    """

    def __init__(self, enable_advanced_analysis: bool = True):
        self.enable_advanced_analysis = enable_advanced_analysis

    def parse(self, text: str) -> ParserServiceOutput:
        result: ParserResult = lex_bteq(text)
        parsed_sql_list: List[ParsedSql] = []
        for block in result.sql_blocks:
            parsed_sql_list.append(self._parse_sql_block(block))
        out = ParserServiceOutput()
        out.controls = result.controls
        out.sql = parsed_sql_list
        return out

    def _parse_sql_block(self, block: SqlBlock) -> ParsedSql:
        try:
            sf_sql = transpile_to_snowflake(block.sql)
            
            # Initialize result
            result = ParsedSql(
                original=block.sql,
                snowflake_sql=sf_sql,
                start_line=block.start_line,
                end_line=block.end_line,
            )
            
            # Add advanced analysis if enabled
            if self.enable_advanced_analysis:
                result.metadata = analyze_sql_metadata(block.sql)
                result.complexity_metrics = get_query_complexity_metrics(block.sql)
                result.teradata_features = extract_teradata_specific_features(block.sql)
                result.syntax_validation = validate_sql_syntax(block.sql)
                
                # Try to optimize (may fail for complex Teradata syntax)
                try:
                    result.optimized_sql = optimize_sql(block.sql)
                except Exception:
                    result.optimized_sql = None
            
            return result
            
        except Exception as exc:  # noqa: BLE001
            return ParsedSql(
                original=block.sql,
                snowflake_sql=None,
                start_line=block.start_line,
                end_line=block.end_line,
                error=str(exc),
            )
