"""
Agentic BTEQ to Snowflake SP Generator

Pure agentic converter that uses LLM directly with raw BTEQ content and context.
No dependency on parsed AST - works with substituted BTEQ and optional deterministic SP as reference.
"""

import logging
from dataclasses import dataclass
from typing import Optional, Dict, Any, List
from pathlib import Path

from .generator_dmst import GeneratedProcedure
from ..llm.integration import create_llm_service
from utils.logging.improved_llm_logger import (
    create_improved_llm_logger, log_bteq_to_sp_request, ConversionType
)
from services.common.prompt_manager import get_prompt_manager

# Pydantic structured output parsing  
from langchain.output_parsers import PydanticOutputParser
from langchain.schema import OutputParserException
from .models import SPGenerationResponse, SPGenerationResponseEnvelope

logger = logging.getLogger(__name__)


@dataclass
class AgenticSPContext:
    """Context for agentic SP generation."""
    original_bteq: str
    substituted_bteq: str
    procedure_name: str
    reference_deterministic_sp: Optional[str] = None
    analysis_notes: Optional[str] = None
    complexity_hints: Optional[Dict[str, Any]] = None


@dataclass
class AgenticGeneratedProcedure(GeneratedProcedure):
    """Agentic SP result with LLM metadata."""
    model_used: str
    confidence_score: float
    llm_enhancements: List[str]
    generation_time_ms: int
    prompt_tokens: Optional[int] = None
    completion_tokens: Optional[int] = None


class SnowflakeSPGeneratorAgtc:
    """Agentic BTEQ to Snowflake SP generator using LLM with raw BTEQ input."""
    
    def __init__(self, llm_log_dir: Optional[str] = None, dry_run: bool = False):
        """
        Initialize agentic SP generator.
        
        Args:
            llm_log_dir: Directory for LLM interaction logging
            dry_run: If True, only log requests without making actual LLM calls
        """
        self.llm_service = create_llm_service(llm_log_dir=llm_log_dir)
        self.llm_log_dir = llm_log_dir
        self.dry_run = dry_run
        
        # Initialize prompt manager for external templates
        self.prompt_manager = get_prompt_manager()
        
        # Initialize improved logger if directory provided
        if llm_log_dir:
            run_dir = Path(llm_log_dir).parent if Path(llm_log_dir).name == "llm_interactions" else Path(llm_log_dir)
            self.improved_logger = create_improved_llm_logger(str(run_dir))
            logger.info(f"✅ Initialized improved LLM logger: {run_dir}/llm_interactions/")
        else:
            self.improved_logger = None
    
    def generate(self, 
                 context: AgenticSPContext, 
                 model_name: str = "claude-4-sonnet") -> AgenticGeneratedProcedure:
        """
        Generate Snowflake stored procedure using LLM with raw BTEQ input.
        
        Args:
            context: Complete agentic generation context
            model_name: LLM model to use
            
        Returns:
            Agentic generated procedure with metadata
        """
        import time
        start_time = time.time()
        
        logger.info(f"🤖 Starting agentic SP generation for {context.procedure_name} using {model_name}")
        
        try:
            # Create Pydantic output parser for structured response
            output_parser = PydanticOutputParser(pydantic_object=SPGenerationResponse)
            
            # Build agentic prompt for SP generation with structured output
            prompt = self._build_sp_generation_prompt(context, output_parser)
            
            # Log the request if improved logger available
            if self.improved_logger:
                log_bteq_to_sp_request(
                    logger=self.improved_logger,
                    procedure_name=context.procedure_name,
                    model=model_name,
                    prompt=prompt,
                    context_data={"substituted_bteq": context.substituted_bteq}
                )
            
            # Generate SP using LLM (or skip in dry-run mode)
            if self.dry_run:
                logger.info(f"🌵 DRY-RUN: Skipping actual LLM call for {context.procedure_name}")
                # Create mock structured response for dry-run with valid SP structure
                mock_sp_sql = f"""CREATE OR REPLACE PROCEDURE {context.procedure_name}(
    ERROR_TABLE STRING DEFAULT 'PROCESS_ERROR_LOG',
    PROCESS_KEY STRING DEFAULT 'DRY_RUN_PROCESS'
)
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
    -- DRY-RUN MODE: This is a mock procedure for testing
    -- In normal mode, this would contain the LLM-generated stored procedure
    RETURN 'DRY-RUN: Mock procedure executed successfully';
END;
$$;"""
                
                structured_response = SPGenerationResponse(
                    procedure_name=context.procedure_name,
                    sql_code=mock_sp_sql,
                    purpose="Dry-run mock procedure for testing Pydantic structured output",
                    parameters=["ERROR_TABLE STRING DEFAULT 'PROCESS_ERROR_LOG'", "PROCESS_KEY STRING DEFAULT 'DRY_RUN_PROCESS'"],
                    business_logic=["Dry-run business logic placeholder", "Mock data processing", "Test structured response"],
                    error_handling=["Dry-run error handling placeholder", "Mock exception handling", "Test validation"],
                    bteq_features_converted=["Mock BTEQ control statements", "Test BTEQ pattern conversion"],
                    snowflake_enhancements=["Mock Snowflake optimizations", "Test SP generation", "Structured output validation"]
                )
                generated_sp = structured_response.sql_code
                enhancements = structured_response.snowflake_enhancements
            else:
                llm_response = self.llm_service.enhance_procedure(
                    prompt, 
                    model=model_name,
                    procedure_name=context.procedure_name,
                    token_count_model="snowflake-arctic-embed-l-v2.0"
                )
                
                if not llm_response:
                    raise RuntimeError(f"LLM returned empty response for {context.procedure_name}")
                
                # Parse structured JSON response
                try:
                    structured_response = output_parser.parse(llm_response)
                    generated_sp = structured_response.sql_code
                    enhancements = structured_response.snowflake_enhancements
                    logger.info(f"✅ Successfully parsed structured response for {context.procedure_name}")
                except OutputParserException as parse_error:
                    logger.warning(f"⚠️ Structured parsing failed for {context.procedure_name}: {parse_error}")
                    logger.info("🔄 Falling back to raw text extraction")
                    # Fallback to raw extraction for compatibility
                    generated_sp = self._extract_sp_from_response(llm_response)
                    enhancements = self._extract_enhancements(llm_response)
                    # Create minimal structured response from raw extraction  
                    structured_response = SPGenerationResponse(
                        procedure_name=context.procedure_name,
                        sql_code=generated_sp,
                        purpose="Extracted from raw LLM response",
                        parameters=["ERROR_TABLE STRING", "PROCESS_KEY STRING"], 
                        business_logic=["Converted from BTEQ script"],
                        error_handling=["Standard error handling"],
                        bteq_features_converted=["Control flow statements"],
                        snowflake_enhancements=enhancements
                    )
            
            # Estimate confidence based on response quality
            confidence = self._estimate_confidence_from_structured(structured_response) if 'structured_response' in locals() else self._estimate_confidence(llm_response)
            
            generation_time = int((time.time() - start_time) * 1000)
            
            logger.info(f"✅ Agentic SP generation completed in {generation_time}ms (confidence: {confidence:.2f})")
            
            # Use structured response data if available, otherwise extract from SP
            if 'structured_response' in locals():
                parameters = structured_response.parameters
                error_handling_info = structured_response.error_handling
            else:
                parameters = self._extract_parameters_from_sp(generated_sp)
                error_handling_info = self._extract_error_handling(generated_sp)
            
            return AgenticGeneratedProcedure(
                name=context.procedure_name,
                sql=generated_sp,
                parameters=parameters,
                error_handling=error_handling_info,
                warnings=[],
                model_used=model_name,
                confidence_score=confidence,
                llm_enhancements=enhancements,
                generation_time_ms=generation_time
            )
            
        except Exception as e:
            logger.error(f"❌ Agentic SP generation failed for {context.procedure_name}: {e}")
            raise
    
    def generate_multi_model(self, 
                            context: AgenticSPContext,
                            models: List[str] = None) -> Dict[str, AgenticGeneratedProcedure]:
        """Generate SP using multiple models for comparison."""
        if models is None:
            models = ["claude-4-sonnet", "snowflake-llama-3.3-70b"]
        
        logger.info(f"🔄 Multi-model agentic SP generation for {context.procedure_name}")
        logger.info(f"Models: {models}")
        
        results = {}
        for model in models:
            try:
                result = self.generate(context, model)
                results[model] = result
                logger.info(f"✅ {model} generation successful")
            except Exception as e:
                logger.error(f"❌ {model} generation failed: {e}")
        
        return results
    
    def _build_sp_generation_prompt(self, context: AgenticSPContext, output_parser: PydanticOutputParser) -> str:
        """Build the agentic prompt for SP generation using external template system with structured output."""
        
        # Use external prompt manager for base prompt
        base_prompt = self.prompt_manager.build_sp_generation_prompt(
            substituted_bteq=context.substituted_bteq,
            procedure_name=context.procedure_name,
            reference_deterministic_sp=context.reference_deterministic_sp or "",
            analysis_notes=context.analysis_notes or "",
            complexity_hints=str(context.complexity_hints) if context.complexity_hints else ""
        )
        
        # Add the Pydantic output schema instructions
        schema_instructions = f"""

The output should be formatted as a JSON instance that conforms to the JSON schema below.

Here is the output schema:
```
{output_parser.get_format_instructions()}
```
"""
        
        return base_prompt + schema_instructions
    
    def _estimate_confidence_from_structured(self, structured_response: SPGenerationResponse) -> float:
        """Estimate confidence based on structured response quality."""
        try:
            score = 0.7  # Base score for successful structured parsing
            
            # Higher score if comprehensive metadata provided
            if len(structured_response.business_logic) >= 3:
                score += 0.1
            if len(structured_response.error_handling) >= 3:
                score += 0.1
            if len(structured_response.bteq_features_converted) >= 2:
                score += 0.05
            if structured_response.migration_notes:
                score += 0.05
            
            return min(score, 1.0)
        except:
            return 0.6
    
    def _extract_sp_from_response(self, response: str) -> str:
        """Extract clean SP SQL from LLM response."""
        # Remove any markdown formatting
        response = response.strip()
        
        # First, try to extract from JSON format (common case)
        if response.startswith('```json') or response.startswith('{'):
            try:
                # Remove markdown wrapper if present
                if response.startswith('```json'):
                    response = response[7:]
                if response.endswith('```'):
                    response = response[:-3]
                    
                # Parse JSON to extract sql_code field
                import json
                try:
                    data = json.loads(response)
                    if isinstance(data, dict) and 'sql_code' in data:
                        return data['sql_code'].strip()
                except json.JSONDecodeError:
                    # JSON might be incomplete, try to extract sql_code manually
                    if '"sql_code":' in response:
                        start = response.find('"sql_code":') + 11
                        # Find the start of the SQL string
                        start = response.find('"', start) + 1
                        if start > 0:
                            # Find the end of the SQL string (accounting for escaped quotes)
                            sql_content = ""
                            i = start
                            while i < len(response):
                                if response[i] == '"' and (i == 0 or response[i-1] != '\\'):
                                    break
                                elif response[i] == '\\' and i + 1 < len(response):
                                    # Handle escaped characters
                                    if response[i+1] == 'n':
                                        sql_content += '\n'
                                        i += 2
                                    elif response[i+1] == 't':
                                        sql_content += '\t'
                                        i += 2
                                    elif response[i+1] == '"':
                                        sql_content += '"'
                                        i += 2
                                    elif response[i+1] == '\\':
                                        sql_content += '\\'
                                        i += 2
                                    else:
                                        sql_content += response[i]
                                        i += 1
                                else:
                                    sql_content += response[i]
                                    i += 1
                            
                            if sql_content.strip():
                                return sql_content.strip()
                        
            except Exception as e:
                logger.warning(f"Failed to extract SQL from JSON response: {e}")
        
        # Fallback: Extract SQL between ```sql blocks if present
        if "```sql" in response:
            start = response.find("```sql") + 6
            end = response.find("```", start)
            if end != -1:
                response = response[start:end]
        elif "```" in response:
            # Handle plain ``` blocks
            start = response.find("```") + 3
            end = response.find("```", start)
            if end != -1:
                response = response[start:end]
        
        return response.strip()
    
    def _extract_enhancements(self, response: str) -> List[str]:
        """Extract LLM enhancements from response."""
        enhancements = []
        
        # Look for common enhancement patterns
        if "error handling" in response.lower():
            enhancements.append("Enhanced error handling")
        if "logging" in response.lower():
            enhancements.append("Added logging capabilities")
        if "performance" in response.lower():
            enhancements.append("Performance optimizations")
        if "best practices" in response.lower():
            enhancements.append("Applied Snowflake best practices")
            
        return enhancements or ["General LLM enhancements"]
    
    def _estimate_confidence(self, response: str) -> float:
        """Estimate confidence based on response characteristics."""
        # Simple heuristic - longer, more structured responses tend to be better
        if len(response) < 500:
            return 0.6
        elif len(response) < 1500:
            return 0.8
        else:
            return 0.9
    
    def _extract_parameters_from_sp(self, sp_sql: str) -> List[str]:
        """Extract parameters from generated SP."""
        # Simple extraction - look for parameter declarations
        parameters = []
        lines = sp_sql.split('\n')
        
        for line in lines:
            line = line.strip().upper()
            if line.startswith('CREATE') and 'PROCEDURE' in line and '(' in line:
                # Extract parameters from CREATE PROCEDURE line
                param_section = line[line.find('(') + 1:line.find(')')]
                if param_section.strip():
                    params = [p.strip().split()[0] for p in param_section.split(',')]
                    parameters.extend(params)
                break
        
        return parameters
    
    def _extract_error_handling(self, sp_sql: str) -> List[str]:
        """Extract error handling patterns from generated SP."""
        error_handling = []
        
        if "EXCEPTION" in sp_sql.upper():
            error_handling.append("Exception handling block")
        if "TRY" in sp_sql.upper() and "CATCH" in sp_sql.upper():
            error_handling.append("Try-catch error handling")
        if "RAISE EXCEPTION" in sp_sql.upper():
            error_handling.append("Custom exception raising")
            
        return error_handling
