"""Parser service package for BTEQ to dbt conversion.

Modules:
- bteq_lexer: splits BTEQ control commands and SQL blocks
- td_sql_parser: SQLGlot-based Teradata SQL parsing utilities
- parser_service: orchestration service combining lexer and SQL parsing
- tokens: typed data structures for the parser IR
- cli: command-line interface for testing
"""
from .tokens import ControlStatement, ControlType, SqlBlock, ParserResult  # noqa: F401
from .parser_service import ParserService  # noqa: F401
