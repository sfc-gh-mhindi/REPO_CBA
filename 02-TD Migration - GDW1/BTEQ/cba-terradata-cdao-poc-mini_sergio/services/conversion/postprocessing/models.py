"""
Data models and configuration for the post-processing service.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Callable, Any
from enum import Enum


class FixType(Enum):
    """Types of fixes that can be applied."""
    DOLLAR_DELIMITER = "dollar_delimiter"
    FILE_RENAME_LOWERCASE = "file_rename_lowercase"
    GET_DIAGNOSTICS = "get_diagnostics"
    OVERLAPS_OPERATOR = "overlaps_operator"
    ICEBERG_SYNTAX = "iceberg_syntax" 
    DATA_TYPE_CONVERSION = "data_type_conversion"
    DEFAULT_CLAUSE_REMOVAL = "default_clause_removal"
    CUSTOM = "custom"


@dataclass
class FixResult:
    """Result of applying a fix."""
    success: bool
    fix_type: FixType
    changes_made: int
    original_content: Optional[str] = None
    modified_content: Optional[str] = None
    error_message: Optional[str] = None
    applied_patterns: List[str] = field(default_factory=list)


@dataclass
class PostProcessorConfig:
    """Configuration for the post-processor service."""
    enabled_fixes: List[FixType] = field(default_factory=lambda: [
        FixType.DOLLAR_DELIMITER,
        FixType.GET_DIAGNOSTICS,
        FixType.OVERLAPS_OPERATOR
    ])
    
    # Dollar delimiter fix settings
    dollar_delimiter_patterns: Dict[str, str] = field(default_factory=lambda: {
        r'\$DOLLAR\$': '$$',
        r'\$\$DELIMITER\$\$': '$$'
    })
    
    # GET DIAGNOSTICS fix settings (PostgreSQL -> Snowflake)
    get_diagnostics_patterns: Dict[str, str] = field(default_factory=lambda: {
        r'GET\s+DIAGNOSTICS\s+(\w+)\s*=\s*ROW_COUNT\s*;': r'\1 := SQLROWCOUNT;',
        r'GET\s+DIAGNOSTICS\s+(\w+)\s*:=\s*ROW_COUNT\s*;': r'\1 := SQLROWCOUNT;'
    })
    
    # OVERLAPS operator fix settings (Teradata -> Snowflake)
    overlaps_patterns: Dict[str, str] = field(default_factory=lambda: {
        r'\(\s*([^,]+)\s*,\s*([^)]+)\s*\)\s+OVERLAPS\s+\(\s*([^,]+)\s*,\s*([^)]+)\s*\)': r'(\1 <= \4 AND \2 >= \3)',
        r'AND\s+\(\s*([^,]+)\s*,\s*([^)]+)\s*\)\s+OVERLAPS\s+\(\s*([^,]+)\s*,\s*([^)]+)\s*\)': r'AND (\1 <= \4 AND \2 >= \3)'
    })
    
    # Data type conversion settings
    data_type_patterns: Dict[str, str] = field(default_factory=lambda: {
        r'\bVARCHAR\(\d+\)': 'STRING',
        r'\bCHAR\(\d+\)': 'STRING',
        r'\bTEXT\b': 'STRING'
    })
    
    # ICEBERG table fix settings
    iceberg_table_patterns: Dict[str, str] = field(default_factory=lambda: {
        r'\bCREATE\s+TABLE\b': 'CREATE ICEBERG TABLE',
        r'\bCREATE\s+OR\s+REPLACE\s+TABLE\b': 'CREATE OR REPLACE ICEBERG TABLE'
    })
    
    # Default clause removal patterns (for ICEBERG tables)
    default_clause_patterns: List[str] = field(default_factory=lambda: [
        r'\s+DEFAULT\s+[^,\s)]+(?=\s*[,)])',  # Remove DEFAULT value
        r',?\s*DEFAULT\s+[^,\s)]+(?=\s*[,)])'  # Remove DEFAULT with optional comma
    ])
    
    # Custom fix patterns
    custom_patterns: Dict[str, str] = field(default_factory=dict)
    
    # Processing settings
    apply_fixes_sequentially: bool = True
    validate_syntax_after_fixes: bool = True
    create_backup: bool = False
    log_all_changes: bool = True


@dataclass
class PostProcessingResult:
    """Result of post-processing operation."""
    success: bool
    original_content: str
    processed_content: str
    fixes_applied: List[FixResult]
    total_changes: int = 0
    processing_time_ms: int = 0
    error_message: Optional[str] = None
    backup_created: bool = False
    
    def __post_init__(self):
        """Calculate total changes after initialization."""
        self.total_changes = sum(fix.changes_made for fix in self.fixes_applied)


@dataclass 
class CustomFix:
    """Custom fix definition."""
    name: str
    description: str
    pattern: str
    replacement: str
    enabled: bool = True
    case_sensitive: bool = False
    use_regex: bool = True
