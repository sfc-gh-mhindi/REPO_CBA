"""
Pydantic models for structured SP conversion responses.
Provides type-safe, validated responses from LLMs for stored procedure generation.
"""

from typing import List, Optional
from pydantic import BaseModel, Field, validator, ConfigDict
import re


class SPGenerationResponse(BaseModel):
    """Structured response for Snowflake stored procedure generation."""
    
    model_config = ConfigDict(protected_namespaces=())
    
    procedure_name: str = Field(
        description="The stored procedure name (e.g., ACCT_BALN_BKDT_DELT_PROC)"
    )
    
    sql_code: str = Field(
        description="Complete Snowflake stored procedure SQL code"
    )
    
    purpose: str = Field(
        description="Brief description of the procedure's business purpose"
    )
    
    parameters: List[str] = Field(
        description="List of procedure parameters with types and descriptions"
    )
    
    business_logic: List[str] = Field(
        description="Key business logic points and transformation details"
    )
    
    error_handling: List[str] = Field(
        description="Error handling patterns implemented in the procedure"
    )
    
    bteq_features_converted: List[str] = Field(
        description="BTEQ features that were converted (e.g., .GOTO, .IF, .LABEL)"
    )
    
    snowflake_enhancements: List[str] = Field(
        description="Snowflake-specific enhancements added beyond the original BTEQ"
    )
    
    migration_notes: Optional[List[str]] = Field(
        default=[],
        description="Important notes about the conversion from BTEQ to Snowflake"
    )
    
    performance_considerations: Optional[List[str]] = Field(
        default=[],
        description="Performance optimization notes and recommendations"
    )
    
    @validator('procedure_name')
    def validate_procedure_name(cls, v):
        """Ensure procedure name follows Snowflake conventions."""
        if not v:
            raise ValueError("procedure_name cannot be empty")
        
        # Should be uppercase with underscores, ending with _PROC
        if not re.match(r'^[A-Z][A-Z0-9_]*_PROC$', v):
            if not v.endswith('_PROC'):
                v = f"{v}_PROC"
            v = v.upper()
        
        return v
    
    @validator('sql_code')
    def validate_sql_code(cls, v):
        """Basic validation of SQL procedure content."""
        if not v or len(v.strip()) < 100:
            raise ValueError("sql_code must contain substantial SQL content")
        
        # Check for basic Snowflake stored procedure patterns
        required_patterns = ['CREATE OR REPLACE PROCEDURE', 'RETURNS', 'LANGUAGE SQL', 'BEGIN']
        for pattern in required_patterns:
            if pattern not in v.upper():
                raise ValueError(f"sql_code must contain '{pattern}'")
        
        return v.strip()
    
    @validator('parameters')
    def validate_parameters(cls, v):
        """Ensure parameters list is valid."""
        if not isinstance(v, list):
            raise ValueError("parameters must be a list")
        
        # Each parameter should be a string describing the parameter
        for param in v:
            if not isinstance(param, str) or len(param.strip()) < 5:
                raise ValueError("Each parameter must be a descriptive string")
        
        return v


class SPConversionError(BaseModel):
    """Error response when SP conversion fails."""
    
    error_type: str = Field(description="Type of error encountered")
    error_message: str = Field(description="Detailed error description") 
    suggestions: List[str] = Field(default=[], description="Suggested fixes")
    bteq_patterns_failed: Optional[List[str]] = Field(
        default=[], description="BTEQ patterns that could not be converted"
    )


class SPGenerationResponseEnvelope(BaseModel):
    """Envelope for SP generation response with error handling."""
    
    success: bool = Field(description="Whether the conversion was successful")
    result: Optional[SPGenerationResponse] = Field(default=None, description="Conversion result if successful")
    error: Optional[SPConversionError] = Field(default=None, description="Error details if failed")
    
    @validator('result', 'error')
    def validate_result_error_exclusivity(cls, v, values):
        """Ensure either result or error is present, not both."""
        success = values.get('success', False)
        
        if success and not values.get('result'):
            raise ValueError("result is required when success=True")
        elif not success and not values.get('error'):
            raise ValueError("error is required when success=False")
        
        return v
