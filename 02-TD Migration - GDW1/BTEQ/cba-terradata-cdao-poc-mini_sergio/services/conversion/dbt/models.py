"""
Pydantic models for structured DBT conversion responses.
Provides type-safe, validated responses from LLMs.
"""

from typing import List, Optional
from pydantic import BaseModel, Field, validator, ConfigDict
import re


class DBTModelResponse(BaseModel):
    """Structured response for DBT model conversion."""
    
    model_config = ConfigDict(protected_namespaces=())
    
    filename: str = Field(
        description="The filename for the DBT model (without .sql extension), derived from the model name in lowercase"
    )
    
    model_name: str = Field(
        description="The business model name extracted from BTEQ procedure name (e.g., ACCT_BALN_BKDT_ADJ_RULE)"
    )
    
    process_name: str = Field(
        description="Process name for logging hooks (e.g., ACCT_BALN_BKDT)"
    )
    
    stream_name: str = Field(
        description="Stream name for logging hooks (e.g., ACCT_BALN_BKDT_ADJ_RULE)"  
    )
    
    dbt_sql: str = Field(
        description="Complete DBT model SQL code with Jinja templating"
    )
    
    purpose: str = Field(
        description="Brief description of the model's business purpose"
    )
    
    business_logic: List[str] = Field(
        description="Key business logic points and transformation details"
    )
    
    dependencies: List[str] = Field(
        description="Source table dependencies using var() constructs"
    )
    
    migration_notes: Optional[List[str]] = Field(
        default=[],
        description="Important notes about the conversion from BTEQ to DBT"
    )
    
    @validator('filename')
    def validate_filename(cls, v):
        """Ensure filename is lowercase and valid."""
        if not v:
            raise ValueError("filename cannot be empty")
        
        # Convert to lowercase and replace invalid characters
        filename = re.sub(r'[^a-z0-9_]', '_', v.lower())
        
        # Ensure it starts with a letter or underscore
        if not re.match(r'^[a-z_]', filename):
            filename = f"model_{filename}"
            
        return filename
    
    @validator('model_name')
    def validate_model_name(cls, v):
        """Ensure model_name follows naming conventions."""
        if not v:
            raise ValueError("model_name cannot be empty")
        
        # Should be uppercase with underscores
        if not re.match(r'^[A-Z][A-Z0-9_]*$', v):
            raise ValueError("model_name must be uppercase with underscores (e.g., ACCT_BALN_BKDT_ADJ_RULE)")
        
        return v
    
    @validator('dbt_sql')
    def validate_dbt_sql(cls, v):
        """Basic validation of DBT SQL content."""
        if not v or len(v.strip()) < 50:
            raise ValueError("dbt_sql must contain substantial SQL content")
        
        # Check for basic DBT patterns
        required_patterns = ['config(', 'SELECT']
        for pattern in required_patterns:
            if pattern not in v:
                raise ValueError(f"dbt_sql must contain '{pattern}'")
        
        return v.strip()


class DBTConversionError(BaseModel):
    """Error response when conversion fails."""
    
    error_type: str = Field(description="Type of error encountered")
    error_message: str = Field(description="Detailed error description") 
    suggestions: List[str] = Field(default=[], description="Suggested fixes")


class DBTModelResponseEnvelope(BaseModel):
    """Envelope for DBT model response with error handling."""
    
    success: bool = Field(description="Whether the conversion was successful")
    result: Optional[DBTModelResponse] = Field(default=None, description="Conversion result if successful")
    error: Optional[DBTConversionError] = Field(default=None, description="Error details if failed")
    
    @validator('result', 'error')
    def validate_result_error_exclusivity(cls, v, values):
        """Ensure either result or error is present, not both."""
        success = values.get('success', False)
        
        if success and not values.get('result'):
            raise ValueError("result is required when success=True")
        elif not success and not values.get('error'):
            raise ValueError("error is required when success=False")
        
        return v
