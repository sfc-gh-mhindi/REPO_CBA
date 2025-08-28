"""
BTEQ to DBT Model Converter

Converts BTEQ SQL scripts to DBT Jinja models using LLM with dual model comparison.
Emphasizes DBT best practices, SQL optimization, and no hallucination policy.
"""

import logging
import time
from dataclasses import dataclass
from typing import Dict, List, Any, Optional, Tuple
from pathlib import Path
import json

# Use current project structure imports
from services.parsing.bteq.tokens import ParserResult
from utils.logging.llm_logger import get_llm_logger, LLMLogger
from utils.config import get_model_manager, TaskType, ModelSelectionStrategy
from utils.langchain_cortex_direct import create_cortex_llm
from generator.llm_integration import SnowflakeLLMService

# Structured output with Pydantic and LangChain
from langchain.output_parsers import PydanticOutputParser
from langchain.schema import OutputParserException
from .dbt_response_models import DBTModelResponse, DBTModelResponseEnvelope

# Improved logging system
from utils.logging.improved_llm_logger import (
    ImprovedLLMLogger, ConversionType, LLMRequest, LLMResponse,
    create_improved_llm_logger, log_bteq_to_dbt_request
)

logger = logging.getLogger(__name__)


@dataclass
class DBTConversionContext:
    """Context for BTEQ to DBT conversion."""
    original_bteq_sql: str
    chosen_stored_procedure: str  # Reference stored procedure translation
    procedure_name: str
    analysis_markdown: Optional[str] = None
    parser_result: Optional[ParserResult] = None


@dataclass
class DBTModelResult:
    """Result of DBT model generation."""
    name: str
    dbt_sql: str
    model: str  # Model name used for generation
    quality_score: float
    generation_time_ms: int
    warnings: List[str]
    dbt_features: List[str]  # DBT features used (materialization, tests, etc.)
    migration_notes: List[str]


@dataclass
class DBTConversionResult:
    """Complete result of dual-model DBT conversion."""
    context: DBTConversionContext
    model_results: Dict[str, DBTModelResult]  # model_name -> result
    preferred_result: DBTModelResult
    comparison_notes: List[str]
    total_processing_time_ms: int


class DBTConverter:
    """
    BTEQ to DBT Model Converter using dual LLM approach.
    
    Converts BTEQ SQL to DBT Jinja models with emphasis on:
    - DBT best practices and conventions
    - SQL optimization for modern data warehouses
    - No hallucination (only transform provided logic)
    - Proper materialization strategies
    - Data quality tests and documentation
    """
    
    def __init__(self, 
                 models: Optional[List[str]] = None,
                 llm_log_dir: Optional[str] = None,
                 output_directory: Optional[Path] = None):
        """
        Initialize DBT converter.
        
        Args:
            models: List of models to use for dual comparison
            llm_log_dir: Directory for LLM interaction logs  
            output_directory: Run-specific output directory for saving interactions
        """
        # Initialize improved logger with unified structure if output_directory provided
        if output_directory:
            # Use improved logging system (creates /llm_interactions/)
            self.improved_logger = create_improved_llm_logger(str(output_directory))
            logger.info(f"✅ Initialized improved LLM logger: {output_directory}/llm_interactions/")
            
            # No need for old LLMLogger when we have improved logging
            self.llm_logger = None
        else:
            # Fallback to old logging system only when no output directory
            self.llm_logger = get_llm_logger()
            self.improved_logger = None
            logger.info("Using fallback LLM logging system")
        
        self.llm_service = SnowflakeLLMService(llm_log_dir)
        self.output_directory = output_directory
        
        # Use default models if none specified  
        if models is None:
            models = ["claude-4-sonnet", "snowflake-llama-3.3-70b"]
            
        logger.info(f"🎯 DBTConverter initializing with models: {models}")
        
        self.models = {}
        for model_name in models:
            try:
                self.models[model_name] = create_cortex_llm(model=model_name)
                logger.info(f"Initialized DBT converter model: {model_name}")
            except Exception as e:
                logger.error(f"Failed to initialize model {model_name}: {e}")
        
        if not self.models:
            raise RuntimeError("No models available for DBT conversion")
            
        logger.info(f"DBT Converter initialized with {len(self.models)} models")
    
    def convert_to_dbt(self, context: DBTConversionContext) -> DBTConversionResult:
        """
        Convert BTEQ SQL to DBT model using dual LLM approach.
        
        Args:
            context: Complete conversion context
            
        Returns:
            Complete conversion result with model comparison
        """
        logger.info(f"Starting BTEQ to DBT conversion for {context.procedure_name}")
        start_time = time.time()
        
        # Generate DBT models using multiple LLMs with structured output
        model_results = {}
        for model_name, llm in self.models.items():
            logger.info(f"Generating DBT model with {model_name}")
            
            # Try structured output first
            model_result = self._generate_dbt_with_structured_output(context, model_name, llm)
            
            # Fallback to original method if structured output fails
            if not model_result:
                logger.warning(f"Structured output failed for {model_name}, falling back to text parsing")
                model_result = self._generate_dbt_with_model(context, model_name, llm)
            
            if model_result:
                model_results[model_name] = model_result
                logger.info(f"✅ {model_name} DBT generation completed")
            else:
                logger.warning(f"⚠️  {model_name} DBT generation failed")
        
        if not model_results:
            raise RuntimeError("All models failed to generate DBT output")
        
        # Compare results and select preferred
        preferred_result, comparison_notes = self._select_preferred_dbt(model_results)
        
        total_time = (time.time() - start_time) * 1000
        
        # Generate final summary if improved logging enabled
        if self.improved_logger:
            summary = self.improved_logger.generate_summary()
            logger.info(f"📊 LLM interactions summary: {summary.get('total_interactions', 0)} total")
        
        result = DBTConversionResult(
            context=context,
            model_results=model_results,
            preferred_result=preferred_result,
            comparison_notes=comparison_notes,
            total_processing_time_ms=int(total_time)
        )
        
        logger.info(f"DBT conversion completed in {total_time:.0f}ms. Preferred: {preferred_result.model}")
        return result
    
    def _generate_dbt_with_model(self, 
                                context: DBTConversionContext, 
                                model_name: str, 
                                llm) -> Optional[DBTModelResult]:
        """Generate DBT model using specified LLM."""
        try:
            start_time = time.time()
            
            # Build DBT-specific prompt
            prompt = self._build_dbt_conversion_prompt(context)
            
            # Use improved logging if available, fallback to old system
            interaction_id = None
            if self.improved_logger:
                # Use improved logging (this is preferred when available)
                interaction_id = log_bteq_to_dbt_request(
                    self.improved_logger,
                    context.procedure_name,
                    model_name,
                    prompt,
                    {"conversion_type": "bteq_to_dbt_fallback"}
                )
            elif self.llm_logger:
                # Fallback to old logging system
                interaction_id = self.llm_logger.start_interaction(
                    provider="snowflake_cortex",
                    model=model_name,
                    request_type="dbt_conversion",
                    prompt=prompt,
                    context_data={
                        "procedure_name": context.procedure_name,
                        "conversion_type": "bteq_to_dbt"
                    }
                )
            else:
                # Ultimate fallback - use global logger
                llm_logger = get_llm_logger()
                interaction_id = llm_logger.start_interaction(
                    provider="snowflake_cortex",
                    model=model_name,
                    request_type="dbt_conversion",
                    prompt=prompt,
                    context_data={
                        "procedure_name": context.procedure_name,
                        "conversion_type": "bteq_to_dbt"
                    }
                )
            
            # Generate DBT model
            response = llm.invoke(prompt)
            generation_time = (time.time() - start_time) * 1000
            
            # Log interaction completion
            if self.improved_logger and interaction_id:
                # Use improved logging
                llm_response = LLMResponse(
                    conversion_type=ConversionType.BTEQ_TO_DBT,
                    procedure_name=context.procedure_name,
                    model=model_name,
                    response=response,
                    success=True,
                    processing_time_ms=int(generation_time),
                    quality_score=0.7  # Default quality score for fallback method
                )
                self.improved_logger.log_response(llm_response, interaction_id)
            elif self.llm_logger:
                # Fallback to old system
                self.llm_logger.complete_interaction(
                    interaction_id=interaction_id,
                    response=response,
                    success=True,
                    processing_time_ms=int(generation_time)
                )
            else:
                # Ultimate fallback
                llm_logger = get_llm_logger()
                llm_logger.complete_interaction(
                    interaction_id=interaction_id,
                    response=response,
                    success=True,
                    processing_time_ms=int(generation_time)
                )
            
            # Parse and validate response
            dbt_result = self._parse_dbt_response(
                response, model_name, int(generation_time), context.procedure_name
            )
            
            return dbt_result
            
        except Exception as e:
            # Log interaction failure with appropriate logging system
            if 'interaction_id' in locals() and interaction_id:
                if self.improved_logger:
                    # Use improved logging for error
                    llm_response = LLMResponse(
                        conversion_type=ConversionType.BTEQ_TO_DBT,
                        procedure_name=context.procedure_name,
                        model=model_name,
                        response=str(e),
                        success=False,
                        processing_time_ms=int((time.time() - start_time) * 1000) if 'start_time' in locals() else 0,
                        quality_score=0.0,
                        error_message=str(e)
                    )
                    self.improved_logger.log_response(llm_response, interaction_id)
                elif self.llm_logger:
                    # Fallback to old system
                    self.llm_logger.complete_interaction(
                        interaction_id=interaction_id,
                        response=str(e),
                        success=False,
                        processing_time_ms=int((time.time() - start_time) * 1000) if 'start_time' in locals() else 0
                    )
                else:
                    # Ultimate fallback
                    llm_logger = get_llm_logger()
                    llm_logger.complete_interaction(
                        interaction_id=interaction_id,
                        response=str(e),
                        success=False,
                        processing_time_ms=int((time.time() - start_time) * 1000) if 'start_time' in locals() else 0
                )
            
            logger.error(f"DBT generation failed with {model_name}: {e}")
            return None
    
    def _generate_dbt_with_structured_output(self,
                                           context: DBTConversionContext,
                                           model_name: str,
                                           llm) -> Optional[DBTModelResult]:
        """
        Generate DBT model using structured output with Pydantic validation.
        
        More reliable than text parsing approach.
        """
        
        try:
            # Create Pydantic output parser
            output_parser = PydanticOutputParser(pydantic_object=DBTModelResponse)
            
            # Build structured prompt
            prompt = self._build_structured_dbt_conversion_prompt(context, output_parser)
            
            logger.info(f"Generating structured DBT model with {model_name}")
            
            # Use improved logging if available, fallback to old system
            if self.improved_logger:
                # Log request with improved system
                interaction_id = log_bteq_to_dbt_request(
                    self.improved_logger,
                    context.procedure_name,
                    model_name,
                    prompt,
                    {"conversion_type": "bteq_to_dbt_structured"}
                )
            else:
                # Fallback to old logging system
                llm_logger = get_llm_logger()
                interaction_id = llm_logger.start_interaction(
                    provider="snowflake_cortex",
                    model=model_name,
                    request_type="dbt_conversion_structured",
                    prompt=prompt,
                    context_data={
                        "procedure_name": context.procedure_name,
                        "conversion_type": "bteq_to_dbt_structured"
                    }
                )
            
            start_time = time.time()
            
            # Generate response with structured output
            response = llm.invoke(prompt)
            
            generation_time = int((time.time() - start_time) * 1000)
            
            # Parse structured response
            try:
                parsed_response = output_parser.parse(response)
                
                # Log successful response with improved system
                if self.improved_logger:
                    llm_response = LLMResponse(
                        conversion_type=ConversionType.BTEQ_TO_DBT,
                        procedure_name=context.procedure_name,
                        model=model_name,
                        response=response,
                        success=True,
                        processing_time_ms=generation_time,
                        quality_score=0.95  # High score for successful structured parsing
                    )
                    self.improved_logger.log_response(llm_response, interaction_id)
                else:
                    # Fallback to old system
                    llm_logger.complete_interaction(
                        interaction_id,
                        response=response,
                        success=True,
                        processing_time_ms=generation_time,
                        quality_score=0.95,
                        enhancements_detected=["structured_output", "pydantic_validation"]
                    )
                
                # Convert to DBTModelResult
                result = self._convert_structured_response_to_dbt_result(
                    parsed_response, model_name, generation_time
                )
                
                logger.info(f"✅ Generated structured DBT model: {result.name}")
                return result
                
            except OutputParserException as parse_error:
                logger.warning(f"Structured parsing failed, falling back to text parsing: {parse_error}")
                
                # Log parsing failure
                if self.improved_logger:
                    llm_response = LLMResponse(
                        conversion_type=ConversionType.BTEQ_TO_DBT,
                        procedure_name=context.procedure_name,
                        model=model_name,
                        response=response,
                        success=False,
                        processing_time_ms=generation_time,
                        quality_score=0.3,
                        error_message=f"Structured parsing failed: {parse_error}"
                    )
                    self.improved_logger.log_response(llm_response, interaction_id)
                else:
                    # Fallback to old system
                    llm_logger.complete_interaction(
                        interaction_id,
                        response=response,
                        success=False,
                        error_message=f"Structured parsing failed: {parse_error}",
                        processing_time_ms=generation_time,
                        quality_score=0.3
                    )
                
                # Fallback to original text parsing method
                return self._parse_dbt_response(response, model_name, generation_time, context.procedure_name)
                
        except Exception as e:
            logger.error(f"Structured DBT generation failed with {model_name}: {e}")
            
            # Log error with improved system if available
            if 'interaction_id' in locals():
                if self.improved_logger:
                    llm_response = LLMResponse(
                        conversion_type=ConversionType.BTEQ_TO_DBT,
                        procedure_name=context.procedure_name,
                        model=model_name,
                        response=str(e),
                        success=False,
                        processing_time_ms=int((time.time() - start_time) * 1000) if 'start_time' in locals() else 0,
                        quality_score=0.0,
                        error_message=str(e)
                    )
                    self.improved_logger.log_response(llm_response, interaction_id)
                else:
                    # Fallback to old system
                    llm_logger = get_llm_logger()
                    llm_logger.complete_interaction(
                        interaction_id,
                        success=False,
                        error_message=str(e),
                        processing_time_ms=int((time.time() - start_time) * 1000) if 'start_time' in locals() else None,
                        quality_score=0.0
                    )
            
            return None
    
    def _convert_structured_response_to_dbt_result(self,
                                                  parsed_response: DBTModelResponse,
                                                  model_name: str,
                                                  generation_time_ms: int) -> DBTModelResult:
        """Convert structured Pydantic response to DBTModelResult."""
        
        # Detect DBT features used
        dbt_features = self._detect_dbt_features(parsed_response.dbt_sql)
        
        # Calculate quality score based on structured data completeness
        quality_score = self._calculate_structured_quality_score(parsed_response)
        
        # Extract warnings
        warnings = self._extract_dbt_warnings(parsed_response.dbt_sql)
        
        return DBTModelResult(
            name=parsed_response.filename,
            dbt_sql=parsed_response.dbt_sql,
            model=model_name,
            quality_score=quality_score,
            generation_time_ms=generation_time_ms,
            warnings=warnings,
            dbt_features=dbt_features,
            migration_notes=parsed_response.migration_notes or []
        )
    
    def _calculate_structured_quality_score(self, response: DBTModelResponse) -> float:
        """Calculate quality score for structured response."""
        score = 0.8  # Base score for successful structured parsing
        
        # Bonus points for completeness
        if response.purpose and len(response.purpose) > 20:
            score += 0.05
        
        if response.business_logic and len(response.business_logic) > 0:
            score += 0.05
        
        if response.dependencies and len(response.dependencies) > 0:
            score += 0.05
        
        if response.migration_notes and len(response.migration_notes) > 0:
            score += 0.05
        
        return min(score, 1.0)
    
    def _build_structured_dbt_conversion_prompt(self, 
                                               context: DBTConversionContext,
                                               output_parser: PydanticOutputParser) -> str:
        """
        Build structured prompt for BTEQ to DBT conversion using Pydantic output parser.
        
        Uses structured output format for reliable response parsing.
        """
        format_instructions = output_parser.get_format_instructions()
        
        return f"""# BTEQ SQL to DBT Model Conversion

You are a senior data engineer expert in converting legacy BTEQ SQL scripts to modern DBT models with Jinja templating.

## CRITICAL INSTRUCTIONS - NO HALLUCINATION POLICY
- ONLY transform the logic provided in the source BTEQ SQL
- DO NOT add new business logic, columns, or transformations
- DO NOT create new table references not in the original
- PRESERVE all existing business logic exactly
- If unclear about any logic, maintain the original approach

## Source Context

### 1. Original BTEQ SQL Script
```sql
{context.original_bteq_sql}
```

### 2. Reference Stored Procedure Translation (for context)
```sql
{context.chosen_stored_procedure}
```

{f'''### 3. Additional Analysis
{context.analysis_markdown}
''' if context.analysis_markdown else ''}

## DBT Conversion Requirements

### 1. DBT Model Structure
- Start with proper Jinja config block using config() macro
- Use appropriate materialization strategy (ibrg_cld_table with truncate-load)
- Add meaningful tags for categorization including 'stream_acct_baln_bkdt'
- Include pre_hook and post_hook for logging with parameterized process/stream names
- Set proper database/schema references using vars

### 2. Configuration Standards (REQUIRED)
- ALWAYS use materialized='ibrg_cld_table'
- ALWAYS include incremental_strategy='truncate-load' when DELETE+INSERT pattern exists  
- ALWAYS use database=var('target_database')
- ALWAYS use schema='pddstg'
- ALWAYS include tmp_database=var('dcf_database')
- ALWAYS include tmp_schema=var('dcf_schema')
- ALWAYS include tmp_relation_type='view'
- Use log_process_start() and log_process_success() macro calls with parameterized process/stream names

### 3. DCF Table Reference Standards
- DO NOT use ref() macro for source tables - use proper database.schema variable constructs
- Transform table references like: `%%VTECH%%.ACCT_BALN_ADJ` → `{{{{ var('vtech_db') }}}}.{{{{ var('vtech_sch') }}}}.ACCT_BALN_ADJ`
- Use var() macro for configuration values and database/schema references

### 4. Model Naming and Documentation
- Extract the model name from the BTEQ procedure name (e.g., ACCT_BALN_BKDT_ADJ_RULE_ISRT → ACCT_BALN_BKDT_ADJ_RULE)
- Use the format "Model: MODEL_NAME" in the comment section
- Include clear purpose and business logic descriptions
- List all dependencies using var() constructs

## Process Name Extraction
For logging hooks, derive process_name and stream_name from the BTEQ procedure name:
- Example: ACCT_BALN_BKDT_ADJ_RULE_ISRT → process_name='ACCT_BALN_BKDT', stream_name='ACCT_BALN_BKDT_ADJ_RULE'

{format_instructions}

Provide a complete, valid JSON response that matches the schema exactly."""

    def _build_dbt_conversion_prompt(self, context: DBTConversionContext) -> str:
        """
        Build comprehensive prompt for BTEQ to DBT conversion.
        
        Emphasizes DBT best practices and no hallucination policy.
        """
        return f"""# BTEQ SQL to DBT Model Conversion

You are a senior data engineer expert in converting legacy BTEQ SQL scripts to modern DBT models with Jinja templating.

## CRITICAL INSTRUCTIONS - NO HALLUCINATION POLICY
- ONLY transform the logic provided in the source BTEQ SQL
- DO NOT add new business logic, columns, or transformations
- DO NOT create new table references not in the original
- PRESERVE all existing business logic exactly
- If unclear about any logic, maintain the original approach

## Source Context

### 1. Original BTEQ SQL Script
```sql
{context.original_bteq_sql}
```

### 2. Reference Stored Procedure Translation (for context)
```sql
{context.chosen_stored_procedure}
```

{f'''### 3. Additional Analysis
{context.analysis_markdown}
''' if context.analysis_markdown else ''}

## DBT Conversion Requirements

### 1. DBT Model Structure
- Start with proper Jinja config block using config() macro
- Use appropriate materialization strategy (table, view, incremental)
- Add meaningful tags for categorization
- Include pre_hook and post_hook for logging if needed
- Set proper database/schema references using vars

### 2. SQL Best Practices
- Use modern SQL patterns (CTEs over subqueries where beneficial)
- Implement proper column naming and aliasing
- Add clear comments explaining business logic
- Use explicit column lists (avoid SELECT *)
- Optimize JOIN patterns and WHERE clause ordering

### 3. DCF Table Reference Standards
- DO NOT use ref() macro for source tables - use proper database.schema variable constructs
- Transform table references like: `%%VTECH%%.ACCT_BALN_ADJ` → `{{ var('vtech_db') }}.{{ var('vtech_sch') }}.ACCT_BALN_ADJ`
- Transform table references like: `%%GDWREF%%.GRD_RPRT_CALR_CLYR` → `{{ var('gdwref_db') }}.{{ var('gdwref_sch') }}.GRD_RPRT_CALR_CLYR`
- Use var() macro for configuration values and database/schema references
- Use proper table aliases (lowercase) after table references
- Add data quality tests where appropriate
- Use DBT's built-in functions for date/time operations

### 4. DCF Materialization Strategy (REQUIRED)
- ALWAYS use materialized='ibrg_cld_table' (never use table, view, or incremental)
- ALWAYS include incremental_strategy='truncate-load' when DELETE+INSERT pattern exists
- ALWAYS include tmp_database=var('dcf_database')
- ALWAYS include tmp_schema=var('dcf_schema')
- ALWAYS include tmp_relation_type='view'

### 5. DCF Configuration Standards (REQUIRED)
- ALWAYS use database=var('target_database')
- ALWAYS use schema='pddstg' (not dynamic schema variables)
- Add meaningful tags for data lineage and process categorization
- Include pre/post hooks for DCF process logging with specific process names
- Use log_process_start() and log_process_success() macro calls with parameterized process/stream names

### 6. Code Organization
- Use clear CTE naming that describes the transformation
- Group related logic into logical CTEs
- Add section comments for major transformations
- Maintain proper indentation and formatting

### 7. Model Documentation
- Use the format "Model: MODEL_NAME" in the comment section
- The model name should be derived from the BTEQ process name (e.g., ACCT_BALN_BKDT_ADJ_RULE_ISRT → ACCT_BALN_BKDT_ADJ_RULE)
- Include clear purpose and business logic descriptions

## Expected DBT Model Output Format

The output should be a complete DBT model file with:

```sql
{{%- set process_name = 'YOUR_PROCESS_NAME' -%}}
{{%- set stream_name = 'YOUR_STREAM_NAME' -%}}

{{{{
  config(
    materialized='ibrg_cld_table',
    incremental_strategy='truncate-load',
    database=var('target_database'),
    schema='pddstg',
    tmp_database=var('dcf_database'),
    tmp_schema=var('dcf_schema'),
    tmp_relation_type='view',
    tags=['account_balance', 'backdated_adjustment', 'core_transform', 'sap_source', 'stream_acct_baln_bkdt'],
    pre_hook=[
        "{{{{ log_process_start('" ~ process_name ~ "', '" ~ stream_name ~ "') }}}}"
    ],
    post_hook=[
        "{{{{ log_process_success('" ~ process_name ~ "', '" ~ stream_name ~ "') }}}}"
    ]
  )
}}}}

/*
    Model: MODEL_NAME_FROM_BTEQ
    Purpose: Brief description of the business logic
    Business Logic: 
    - Key transformation details
    - Important business rules
    Dependencies: 
    - {{ var('source_db') }}.{{ var('source_sch') }}.SOURCE_TABLE
*/

WITH cte_name AS (
    SELECT
        col1,
        col2
    FROM {{{{ var('source_db') }}}}.{{{{ var('source_sch') }}}}.SOURCE_TABLE alias
    WHERE condition
),

final AS (
    SELECT ...
)

SELECT * FROM final
```

## Specific Transformation Guidance

### BTEQ Control Flow → DBT Patterns
- Convert DELETE + INSERT patterns to proper materialization
- Replace BTEQ variables with DBT vars and Jinja variables
- Transform error handling to DBT test patterns where possible
- Convert procedural logic to declarative SQL transformations

### Date/Time Functions
- Use DBT date functions or modern SQL equivalents
- Replace Teradata date arithmetic with standard functions
- Maintain timezone handling if present

### Process Control Tables
- Reference process control tables using database.schema variable constructs: {{ var('control_db') }}.{{ var('control_sch') }}.TABLE_NAME
- Use var() for process configuration values and database/schema references
- Implement delta processing via incremental materialization with 'truncate-load' strategy

## Output Instructions

Provide your response in the following structured format:

### FILENAME
[model_name_in_lowercase].sql

### DBT_MODEL
[Complete DBT model SQL code]

Extract the model name from the BTEQ procedure name (e.g., ACCT_BALN_BKDT_ADJ_RULE_ISRT → ACCT_BALN_BKDT_ADJ_RULE) and use it in lowercase for the filename.

Do not include additional explanations, markdown code blocks, or commentary.

## Response:"""

    def _parse_dbt_response(self, 
                           response: str, 
                           model_name: str, 
                           generation_time_ms: int,
                           procedure_name: str) -> DBTModelResult:
        """Parse and validate DBT model response."""
        
        # Parse structured response to get filename and SQL
        filename, dbt_sql = self._parse_structured_response(response)
        
        # If no filename was extracted from response, use procedure_name as fallback
        if not filename:
            filename = procedure_name.lower()
        
        # Detect DBT features used
        dbt_features = self._detect_dbt_features(dbt_sql)
        
        # Calculate quality score
        quality_score = self._calculate_dbt_quality_score(dbt_sql, model_name)
        
        # Extract warnings
        warnings = self._extract_dbt_warnings(dbt_sql)
        
        # Generate migration notes
        migration_notes = self._generate_migration_notes(dbt_sql, procedure_name)
        
        return DBTModelResult(
            name=filename,
            dbt_sql=dbt_sql,
            model=model_name,
            quality_score=quality_score,
            generation_time_ms=generation_time_ms,
            warnings=warnings,
            dbt_features=dbt_features,
            migration_notes=migration_notes
        )
    
    def _parse_structured_response(self, response: str) -> tuple[str, str]:
        """Parse structured response format into filename and SQL."""
        response = response.strip()
        
        # Look for ### FILENAME and ### DBT_MODEL sections
        if "### FILENAME" in response and "### DBT_MODEL" in response:
            parts = response.split("### FILENAME")
            if len(parts) > 1:
                filename_section = parts[1].split("### DBT_MODEL")[0].strip()
                dbt_model_section = response.split("### DBT_MODEL")[1].strip()
                
                # Extract filename (remove .sql extension if present)
                filename = filename_section.replace(".sql", "").strip()
                
                return filename, dbt_model_section
        
        # Fallback to old parsing method if structured format not found
        cleaned_sql = self._clean_dbt_response_legacy(response)
        # Try to extract filename from Model comment in SQL
        model_name = self._extract_model_name_from_sql(cleaned_sql)
        if model_name:
            return model_name.lower(), cleaned_sql
        
        # Ultimate fallback - use procedure name
        return "", cleaned_sql
    
    def _clean_dbt_response_legacy(self, response: str) -> str:
        """Legacy method to clean DBT response (fallback)."""
        # Remove markdown code blocks if present
        if "```sql" in response:
            parts = response.split("```sql")
            if len(parts) > 1:
                sql_part = parts[1].split("```")[0]
                return sql_part.strip()
        
        if "```" in response:
            parts = response.split("```")
            if len(parts) >= 3:
                return parts[1].strip()
        
        # Remove common prefixes
        response = response.strip()
        if response.startswith("## DBT Model:"):
            response = response.replace("## DBT Model:", "").strip()
        
        return response
    
    def _extract_model_name_from_sql(self, sql: str) -> str:
        """Extract model name from SQL comment."""
        import re
        match = re.search(r'Model:\s*([A-Z_][A-Z0-9_]*)', sql, re.IGNORECASE)
        if match:
            return match.group(1)
        return ""
    
    def _detect_dbt_features(self, dbt_sql: str) -> List[str]:
        """Detect DBT features used in the model."""
        features = []
        
        dbt_patterns = {
            "config": "config(",
            "variables": "var(",
            "references": "ref(",
            "sources": "source(",
            "macros": "{{",
            "jinja_variables": "{%-",
            "incremental": "incremental",
            "pre_hooks": "pre_hook",
            "post_hooks": "post_hook",
            "tags": "tags=",
            "tests": "test",
        }
        
        for feature, pattern in dbt_patterns.items():
            if pattern in dbt_sql:
                features.append(feature)
        
        return features
    
    def _calculate_dbt_quality_score(self, dbt_sql: str, model_name: str) -> float:
        """Calculate quality score for DBT model."""
        score = 0.0
        max_score = 1.0
        
        # Base score for model-specific strengths
        model_scores = {
            "claude-4-sonnet": 0.9,
            "claude-3-5-sonnet": 0.85,
            "snowflake-llama-3.3-70b": 0.75,
        }
        score = model_scores.get(model_name, 0.7)
        
        # Bonus points for DBT best practices
        if "config(" in dbt_sql:
            score += 0.05
        if "var(" in dbt_sql:
            score += 0.03
        if "ref(" in dbt_sql:
            score += 0.03
        if "WITH " in dbt_sql.upper():
            score += 0.02
        if "tags=" in dbt_sql:
            score += 0.02
        if "pre_hook" in dbt_sql or "post_hook" in dbt_sql:
            score += 0.02
        if "/*" in dbt_sql and "*/" in dbt_sql:  # Documentation comments
            score += 0.02
        
        return min(score, max_score)
    
    def _extract_dbt_warnings(self, dbt_sql: str) -> List[str]:
        """Extract potential warnings from DBT model."""
        warnings = []
        
        if "SELECT *" in dbt_sql.upper():
            warnings.append("Uses SELECT * - consider explicit column selection")
        
        if "config(" not in dbt_sql:
            warnings.append("Missing config() block - add materialization strategy")
        
        if "var(" not in dbt_sql and ("database" in dbt_sql.lower() or "schema" in dbt_sql.lower()):
            warnings.append("Hardcoded database/schema - consider using var() macro")
        
        if "incremental" in dbt_sql and "is_incremental()" not in dbt_sql:
            warnings.append("Incremental materialization without is_incremental() check")
        
        return warnings
    
    def _generate_migration_notes(self, dbt_sql: str, procedure_name: str) -> List[str]:
        """Generate migration notes for the DBT conversion."""
        notes = []
        
        if "DATEADD" in dbt_sql.upper() or "DATEDIFF" in dbt_sql.upper():
            notes.append("Converted Teradata date functions to Snowflake equivalents")
        
        if "WITH " in dbt_sql.upper():
            notes.append("Organized logic using CTEs for better readability")
        
        if "config(" in dbt_sql:
            notes.append("Added DBT configuration for materialization and metadata")
        
        if "var(" in dbt_sql:
            notes.append("Used DBT variables for environment-specific configuration")
        
        notes.append(f"Converted from procedural BTEQ to declarative DBT model")
        
        return notes
    
    def _select_preferred_dbt(self, 
                             model_results: Dict[str, DBTModelResult]) -> Tuple[DBTModelResult, List[str]]:
        """Select preferred DBT model from multiple results."""
        comparison_notes = []
        
        # Sort by quality score
        sorted_results = sorted(
            model_results.items(), 
            key=lambda x: x[1].quality_score, 
            reverse=True
        )
        
        preferred_model_name, preferred_result = sorted_results[0]
        comparison_notes.append(f"Selected {preferred_model_name} as preferred model")
        
        # Add comparison details
        for model_name, result in sorted_results:
            comparison_notes.append(
                f"{model_name}: Quality={result.quality_score:.3f}, "
                f"Features={len(result.dbt_features)}, "
                f"Time={result.generation_time_ms}ms"
            )
        
        # Check for significant quality differences
        if len(sorted_results) > 1:
            second_score = sorted_results[1][1].quality_score
            quality_diff = preferred_result.quality_score - second_score
            
            if quality_diff > 0.1:
                comparison_notes.append(f"Clear quality advantage: +{quality_diff:.3f}")
            elif quality_diff < 0.05:
                comparison_notes.append("Close quality scores - selection based on features")
        
        return preferred_result, comparison_notes
    

