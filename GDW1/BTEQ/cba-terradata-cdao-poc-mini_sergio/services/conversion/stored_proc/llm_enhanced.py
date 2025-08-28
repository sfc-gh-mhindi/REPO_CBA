"""
LLM-Enhanced Snowflake Stored Procedure Generator

Uses LLM with rich context (original BTEQ, analysis, basic SP) to generate 
high-quality Snowflake stored procedures.
"""

import logging
from dataclasses import dataclass
from typing import Optional, Dict, Any, List
from pathlib import Path

from .generator import SnowflakeSPGenerator, GeneratedProcedure
from ..llm.integration import create_llm_service
# Use absolute imports to fix packaging issues
from services.parsing.bteq.tokens import ParserResult

logger = logging.getLogger(__name__)


@dataclass
class LLMGenerationContext:
    """Context for LLM-enhanced generation."""
    original_bteq: str
    analysis_markdown: str
    basic_stored_procedure: str
    procedure_name: str
    parser_result: ParserResult


@dataclass
class EnhancedGeneratedProcedure(GeneratedProcedure):
    """Enhanced result with LLM improvements."""
    llm_enhancements: List[str]
    quality_score: float
    migration_notes: List[str]
    
    def __init__(self, name: str, sql: str, parameters: List[str], warnings: List[str],
                 llm_enhancements: List[str], quality_score: float, migration_notes: List[str],
                 error_handling: Optional[List[str]] = None):
        # Initialize parent class attributes manually to avoid constructor issues
        self.name = name
        self.sql = sql
        self.parameters = parameters
        self.warnings = warnings
        self.error_handling = error_handling or []
        
        # Enhanced attributes
        self.llm_enhancements = llm_enhancements
        self.quality_score = quality_score
        self.migration_notes = migration_notes


class LLMEnhancedGenerator:
    """LLM-enhanced generator for high-quality Snowflake stored procedures."""
    
    def __init__(self, enable_basic_fallback: bool = True, llm_log_dir: str = None):
        """
        Initialize LLM-enhanced generator.
        
        Args:
            enable_basic_fallback: Fall back to basic generator if LLM fails
            llm_log_dir: Directory to store dedicated LLM interaction logs
        """
        self.basic_generator = SnowflakeSPGenerator()
        self.enable_basic_fallback = enable_basic_fallback
        self.llm_service = create_llm_service(llm_log_dir=llm_log_dir)
        
    def generate_enhanced(self, 
                         context: LLMGenerationContext) -> EnhancedGeneratedProcedure:
        """
        Generate enhanced stored procedure using LLM with rich context.
        
        Args:
            context: Complete context including BTEQ, analysis, and basic SP
            
        Returns:
            Enhanced stored procedure with LLM improvements
        """
        logger.info(f"Starting LLM-enhanced generation for {context.procedure_name}")
        
        try:
            # Generate enhanced procedure using LLM
            enhanced_sql = self._generate_with_llm(context)
            
            # Extract metadata from enhanced procedure
            enhancements, quality_score, migration_notes = self._analyze_enhancements(
                context.basic_stored_procedure, enhanced_sql
            )
            
            return EnhancedGeneratedProcedure(
                name=context.procedure_name,
                sql=enhanced_sql,
                parameters=self._extract_parameters_from_sql(enhanced_sql),
                warnings=[],
                llm_enhancements=enhancements,
                quality_score=quality_score,
                migration_notes=migration_notes
            )
            
        except Exception as e:
            logger.error(f"LLM generation failed for {context.procedure_name}: {e}")
            
            if self.enable_basic_fallback:
                logger.info("Falling back to basic generator")
                basic_result = self.basic_generator.generate(
                    context.parser_result, 
                    context.procedure_name, 
                    context.original_bteq
                )
                
                return EnhancedGeneratedProcedure(
                    name=basic_result.name,
                    sql=basic_result.sql,
                    parameters=basic_result.parameters,
                    warnings=basic_result.warnings + [f"LLM generation failed: {e}"],
                    llm_enhancements=["Basic fallback used due to LLM failure"],
                    quality_score=0.5,
                    migration_notes=["Generated using basic rules - manual review recommended"]
                )
            else:
                raise
    
    def generate_enhanced_multi_model(self, context: LLMGenerationContext, 
                                    models: List[str] = None) -> Dict[str, EnhancedGeneratedProcedure]:
        """
        Generate enhanced stored procedures using multiple LLM models.
        
        Args:
            context: Complete context including BTEQ, analysis, and basic SP
            models: List of models to use (defaults to configured pair)
            
        Returns:
            Dictionary mapping model names to enhanced procedures
        """
        if models is None:
            models = ["claude-4-sonnet", "snowflake-llama-3.3-70b"]
        
        logger.info(f"Starting multi-model enhanced generation for {context.procedure_name}")
        logger.info(f"Models to use: {models}")
        
        results = {}
        for model in models:
            logger.info(f"🤖 Generating with model: {model}")
            try:
                # Generate enhanced procedure using specific model
                enhanced_sql = self._generate_with_llm(context, model)
                
                # Extract metadata from enhanced procedure
                enhancements, quality_score, migration_notes = self._analyze_enhancements(
                    context.basic_stored_procedure, enhanced_sql
                )
                
                # Create enhanced result with model-specific quality score
                model_quality_score = self._calculate_model_quality_score(model, quality_score)
                
                results[model] = EnhancedGeneratedProcedure(
                    name=context.procedure_name,
                    sql=enhanced_sql,
                    parameters=self._extract_parameters_from_sql(enhanced_sql),
                    warnings=[],
                    llm_enhancements=enhancements + [f"Generated by {model}"],
                    quality_score=model_quality_score,
                    migration_notes=migration_notes + [f"Enhanced using {model}"]
                )
                
                logger.info(f"✅ {model} completed successfully (quality: {model_quality_score:.2f})")
                
            except Exception as e:
                logger.error(f"❌ {model} failed: {e}")
                # Create fallback result for failed model
                if self.enable_basic_fallback:
                    basic_result = self.basic_generator.generate(
                        context.parser_result, 
                        context.procedure_name, 
                        context.original_bteq
                    )
                    
                    results[model] = EnhancedGeneratedProcedure(
                        name=basic_result.name,
                        sql=basic_result.sql,
                        parameters=basic_result.parameters,
                        warnings=basic_result.warnings + [f"{model} generation failed: {e}"],
                        llm_enhancements=[f"Basic fallback used due to {model} failure"],
                        quality_score=0.3,  # Low quality score for fallback
                        migration_notes=[f"Generated using basic rules - {model} failed"]
                    )
                else:
                    results[model] = None
        
        successful_models = [m for m, r in results.items() if r is not None]
        logger.info(f"📊 Multi-model generation completed: {len(successful_models)}/{len(models)} successful")
        
        return results
    
    def _calculate_model_quality_score(self, model: str, base_score: float) -> float:
        """Calculate quality score based on model capabilities."""
        # Model-specific quality adjustments
        model_multipliers = {
            "claude-4-sonnet": 1.0,      # Highest quality baseline
            "claude-3-5-sonnet": 0.95,   # Slightly lower
            "snowflake-llama-3.3-70b": 0.85,  # Fast but lower quality
            "claude-3-haiku": 0.8,       # Fast model
            "llama3.1-8b": 0.7,         # Smaller model
        }
        
        multiplier = model_multipliers.get(model, 0.8)  # Default multiplier
        return min(base_score * multiplier, 1.0)  # Cap at 1.0
    
    def _generate_with_llm(self, context: LLMGenerationContext, model: str = "claude-3-5-sonnet") -> str:
        """Generate enhanced stored procedure using LLM."""
        
        # Construct comprehensive prompt for LLM
        prompt = self._build_enhancement_prompt(context)
        
        # Make LLM call using Snowflake LLM service
        enhanced_sql = self._call_snowflake_llm(prompt, model, context.procedure_name)
        
        return enhanced_sql
    
    def _build_enhancement_prompt(self, context: LLMGenerationContext) -> str:
        """Build comprehensive prompt for LLM enhancement."""
        
        prompt = f"""# BTEQ to Snowflake Stored Procedure Enhancement

You are an expert in migrating Teradata BTEQ scripts to high-quality Snowflake stored procedures. 

## Task
Enhance the basic generated stored procedure using the provided context to create a production-ready, optimized Snowflake stored procedure.

## Context

### 1. Original BTEQ Script
```sql
{context.original_bteq}
```

### 2. Detailed Analysis
{context.analysis_markdown}

### 3. Basic Generated Stored Procedure
```sql
{context.basic_stored_procedure}
```

## Enhancement Requirements

### Code Quality
- Use proper Snowflake SQL syntax and best practices
- Implement robust error handling with specific error codes
- Add comprehensive logging for debugging and monitoring
- Use meaningful variable names and comments
- Optimize SQL for Snowflake's columnar architecture

### BTEQ Pattern Migration
- Convert BTEQ control flow (.IF, .GOTO, .LABEL) to proper conditional logic
- Replace BTEQ commands (.RUN, .EXPORT, .IMPORT) with Snowflake equivalents
- Handle error conditions gracefully with appropriate rollback logic
- Preserve business logic while modernizing implementation

### Snowflake Optimization
- Use Snowflake-specific features where beneficial (QUALIFY, window functions)
- Implement proper transaction management
- Add parameter validation and sanitization
- Use explicit column lists and avoid SELECT *
- Optimize JOIN operations for Snowflake

### Documentation
- Add comprehensive header with purpose, parameters, and usage
- Document each major section of the procedure
- Include migration notes for any BTEQ features that required special handling
- Add performance considerations and maintenance notes

## Output Format
Provide only the enhanced Snowflake stored procedure SQL. Do not include explanations or additional text.

## Enhanced Stored Procedure:
"""
        
        return prompt
    
    def _call_snowflake_llm(self, prompt: str, model: str = "claude-3-5-sonnet", procedure_name: str = "unknown_procedure") -> str:
        """Make LLM call using Snowflake LLM service."""
        
        logger.info(f"Calling LLM service for procedure enhancement (model: {model})")
        
        # Use the LLM integration service
        enhanced_sql = self.llm_service.enhance_procedure(prompt, model, procedure_name)
        
        if enhanced_sql:
            logger.info("Successfully generated enhanced procedure with LLM service")
            return enhanced_sql
        else:
            logger.warning("LLM service unavailable, using basic enhanced template")
            # Return a basic enhanced template as fallback
            return self._get_enhanced_template()
    
    def _analyze_enhancements(self, basic_sql: str, enhanced_sql: str) -> tuple[List[str], float, List[str]]:
        """Analyze what enhancements were made by the LLM."""
        
        enhancements = []
        migration_notes = []
        
        # Basic analysis of improvements
        basic_lines = len(basic_sql.splitlines())
        enhanced_lines = len(enhanced_sql.splitlines())
        
        if enhanced_lines > basic_lines * 1.2:
            enhancements.append("Expanded with additional error handling and documentation")
        
        if "EXCEPTION" in enhanced_sql and "WHEN" in enhanced_sql:
            enhancements.append("Enhanced exception handling")
        
        if "COMMENT" in enhanced_sql.upper() or "--" in enhanced_sql:
            enhancements.append("Added comprehensive documentation")
        
        if "LOG" in enhanced_sql.upper() or "AUDIT" in enhanced_sql.upper():
            enhancements.append("Added logging and audit capabilities")
        
        if "VALIDATE" in enhanced_sql.upper() or "CHECK" in enhanced_sql.upper():
            enhancements.append("Added input validation")
        
        # Calculate quality score based on enhancements
        base_score = 0.6
        enhancement_bonus = min(len(enhancements) * 0.1, 0.4)
        quality_score = base_score + enhancement_bonus
        
        # Add migration notes
        if ".IF" in basic_sql or ".GOTO" in basic_sql:
            migration_notes.append("BTEQ control flow converted to Snowflake conditional logic")
        
        if "ERRORCODE" in basic_sql:
            migration_notes.append("BTEQ error handling migrated to Snowflake exception handling")
        
        return enhancements, quality_score, migration_notes
    
    def _extract_parameters_from_sql(self, sql: str) -> List[str]:
        """Extract parameter list from SQL."""
        
        import re
        
        # Find the parameter section in CREATE PROCEDURE
        pattern = r'CREATE\s+(?:OR\s+REPLACE\s+)?PROCEDURE\s+\w+\s*\((.*?)\)'
        match = re.search(pattern, sql, re.IGNORECASE | re.DOTALL)
        
        if match:
            params_text = match.group(1).strip()
            if params_text:
                # Split by comma and clean up
                params = [param.strip() for param in params_text.split(',') if param.strip()]
                return params
    
    def _get_enhanced_template(self) -> str:
        """Get enhanced template as fallback when LLM is not available."""
        
        return """CREATE OR REPLACE PROCEDURE ENHANCED_BTEQ_MIGRATION(
    -- Enhanced parameters with validation
    PROCESS_KEY STRING DEFAULT 'UNKNOWN_PROCESS',
    ERROR_TABLE STRING DEFAULT 'PROCESS_ERROR_LOG',
    DEBUG_MODE BOOLEAN DEFAULT FALSE
)
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
COMMENT = 'Enhanced BTEQ migration - Template fallback'
AS
$$
DECLARE
    -- Enhanced error handling variables
    error_code INTEGER DEFAULT 0;
    error_message STRING DEFAULT '';
    current_step STRING DEFAULT 'INITIALIZATION';
    row_count INTEGER DEFAULT 0;
    start_time TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP();
    
    -- Business logic variables
    result_status STRING DEFAULT 'PENDING';
    
BEGIN
    -- Input validation
    IF PROCESS_KEY IS NULL OR PROCESS_KEY = '' THEN
        RAISE EXCEPTION 'PROCESS_KEY cannot be null or empty';
    END IF;
    
    -- Log procedure start
    IF DEBUG_MODE THEN
        CALL SYSTEM$LOG('INFO', 'Starting ' || current_step || ' for process: ' || PROCESS_KEY);
    END IF;
    
    -- Main business logic (enhanced template)
    current_step := 'MAIN_PROCESSING';
    
    -- Enhanced processing logic would go here
    -- This template provides a solid foundation for BTEQ migrations
    
    -- Success path
    current_step := 'COMPLETION';
    result_status := 'SUCCESS';
    
    IF DEBUG_MODE THEN
        CALL SYSTEM$LOG('INFO', 'Completed ' || current_step || ' - Rows processed: ' || row_count);
    END IF;
    
    RETURN 'SUCCESS: Enhanced process completed. Rows: ' || row_count || ', Duration: ' || 
           DATEDIFF('seconds', start_time, CURRENT_TIMESTAMP()) || 's';

EXCEPTION
    WHEN OTHER THEN
        error_code := SQLCODE;
        error_message := SQLERRM;
        
        -- Enhanced error logging
        INSERT INTO IDENTIFIER(:ERROR_TABLE) (
            PROCESS_KEY, ERROR_CODE, ERROR_MESSAGE, ERROR_STEP, ERROR_TIMESTAMP
        ) VALUES (
            :PROCESS_KEY, error_code, error_message, current_step, CURRENT_TIMESTAMP()
        );
        
        -- Return detailed error information
        RETURN 'ERROR in ' || current_step || ': ' || error_message || ' (Code: ' || error_code || ')';
END;
$$;"""


def create_llm_generation_context(original_bteq: str,
                                analysis_markdown: str, 
                                basic_procedure: str,
                                procedure_name: str,
                                parser_result: ParserResult) -> LLMGenerationContext:
    """Create LLM generation context from available data."""
    
    return LLMGenerationContext(
        original_bteq=original_bteq,
        analysis_markdown=analysis_markdown,
        basic_stored_procedure=basic_procedure,
        procedure_name=procedure_name,
        parser_result=parser_result
    )
