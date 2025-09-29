"""
Post-processing service for Snowflake stored procedure conversions.

This module provides a generic framework for applying common fixes to generated
stored procedures, handling recurring syntax issues like dollar delimiters,
data type conversions, and ICEBERG table compatibility.

Usage:
    from services.conversion.postprocessing import PostProcessingService, PostProcessorConfig
    
    # Use default configuration
    processor = PostProcessingService()
    result = processor.process_file("path/to/stored_procedure.sql")
    
    # Use custom configuration
    config = PostProcessorConfig(enabled_fixes=[FixType.DOLLAR_DELIMITER])
    processor = PostProcessingService(config)
    result = processor.process_content(sql_content)
"""

from .service import PostProcessingService
from .models import (
    PostProcessorConfig,
    PostProcessingResult,
    FixResult,
    FixType,
    CustomFix
)
from .fixes import create_fix, BaseFix

__all__ = [
    'PostProcessingService',
    'PostProcessorConfig', 
    'PostProcessingResult',
    'FixResult',
    'FixType',
    'CustomFix',
    'create_fix',
    'BaseFix'
]

# Version info
__version__ = "1.0.0"
__author__ = "BTEQ Migration Team"
