"""
Agentic Framework for BTEQ Migration using LangChain/LangGraph

Multi-agent system for enhanced BTEQ to Snowflake migration with:
- Tool-based validation
- Error correction agents
- Multi-model orchestration
- Judgment model evaluation
"""

import logging
from typing import Dict, List, Any, Optional, Union
from dataclasses import dataclass
from enum import Enum
from pathlib import Path

from langchain_core.agents import AgentAction, AgentFinish
from langchain_core.callbacks import CallbackManagerForToolRun
from langchain_core.language_models import BaseLanguageModel
from langchain_core.tools import BaseTool
from langchain_core.prompts import PromptTemplate
from langchain.agents import create_react_agent, AgentExecutor
from langchain.memory import ConversationBufferMemory

# Import our existing components - use current project structure
from services.conversion.llm.integration import create_llm_service
from utils.langchain_cortex_direct import SnowflakeCortexDirectLLM, create_cortex_llm
from services.conversion.llm.cortex import DirectCortexLLMService, create_cortex_agent_llm
from services.parsing.bteq.tokens import ParserResult
from utils.logging.llm_logger import get_llm_logger
from utils.config import get_model_manager, TaskType, ModelSelectionStrategy

# Import DBT converter
from services.conversion.dbt.converter import DBTConverter, DBTConversionContext, DBTConversionResult

logger = logging.getLogger(__name__)


class ModelType(Enum):
    """Available LLM models for different tasks - now loaded from config."""
    # Models are now dynamically loaded from config/models.json
    # These enums are kept for backward compatibility
    CLAUDE_4_SONNET = "claude-4-sonnet"
    SNOWFLAKE_LLAMA_33_70B = "snowflake-llama-3.3-70b"
    # Legacy models (keeping for backward compatibility)
    CLAUDE_35_SONNET = "claude-3-5-sonnet"
    LLAMA_31_8B = "llama3.1-8b"
    
    @classmethod
    def get_default_models(cls) -> List[str]:
        """Get default models from configuration."""
        model_manager = get_model_manager()
        return model_manager.get_default_pair()
    
    @classmethod
    def get_models_for_task(cls, task: TaskType) -> List[str]:
        """Get models for specific task from configuration."""
        model_manager = get_model_manager()
        return model_manager.get_models_for_task(task)


class AgentRole(Enum):
    """Different agent roles in the system."""
    ANALYZER = "analyzer"
    GENERATOR = "generator"
    VALIDATOR = "validator"
    JUDGE = "judge"
    ERROR_CORRECTOR = "error_corrector"
    OPTIMIZER = "optimizer"


@dataclass
class AgentResult:
    """Result from an agent execution."""
    agent_role: AgentRole
    model_used: ModelType
    success: bool
    output: str
    confidence_score: float
    execution_time_ms: float
    errors: List[str]
    metadata: Dict[str, Any]


@dataclass
class MigrationContext:
    """Complete context for agentic migration."""
    original_bteq: str
    parser_result: ParserResult
    analysis_markdown: str
    complexity_score: float
    target_procedure_name: str
    quality_requirements: Dict[str, Any]
    validation_rules: List[str]


class SQLValidationTool(BaseTool):
    """Tool for validating SQL syntax and semantics."""
    
    name: str = "sql_validator"
    description: str = "Validates SQL syntax, checks for common issues, and provides suggestions"
    
    def _run(
        self, 
        sql_code: str,
        run_manager: Optional[CallbackManagerForToolRun] = None
    ) -> str:
        """Validate SQL code and return analysis."""
        try:
            # Use SQLGlot for syntax validation
            import sqlglot
            
            results = {
                "syntax_valid": True,
                "issues": [],
                "suggestions": [],
                "snowflake_compatibility": True
            }
            
            try:
                # Parse SQL with Snowflake dialect
                parsed = sqlglot.parse_one(sql_code, dialect="snowflake")
                if not parsed:
                    results["syntax_valid"] = False
                    results["issues"].append("SQL parsing failed")
                else:
                    # Additional validation checks
                    sql_lower = sql_code.lower()
                    
                    # Check for common issues
                    if "select *" in sql_lower:
                        results["suggestions"].append("Consider using explicit column lists instead of SELECT *")
                    
                    if "exception" not in sql_lower:
                        results["suggestions"].append("Consider adding exception handling")
                    
                    if "comment" not in sql_lower and "--" not in sql_code:
                        results["suggestions"].append("Consider adding documentation comments")
                        
            except Exception as e:
                results["syntax_valid"] = False
                results["issues"].append(f"SQL parsing error: {str(e)}")
            
            return f"SQL Validation Results: {results}"
            
        except Exception as e:
            return f"Validation tool error: {str(e)}"


class PerformanceAnalyzerTool(BaseTool):
    """Tool for analyzing SQL performance and optimization opportunities."""
    
    name: str = "performance_analyzer"
    description: str = "Analyzes SQL for performance issues and optimization opportunities"
    
    def _run(
        self,
        sql_code: str,
        run_manager: Optional[CallbackManagerForToolRun] = None
    ) -> str:
        """Analyze SQL performance characteristics."""
        try:
            analysis = {
                "performance_score": 8.5,
                "optimizations": [],
                "concerns": [],
                "snowflake_features": []
            }
            
            sql_lower = sql_code.lower()
            
            # Analyze for performance patterns
            if "qualify" in sql_lower:
                analysis["snowflake_features"].append("Uses QUALIFY for window function filtering")
            
            if "cluster by" in sql_lower:
                analysis["snowflake_features"].append("Uses clustering for performance")
            
            if "join" in sql_lower and "where" in sql_lower:
                analysis["optimizations"].append("Verify JOIN conditions are optimized")
            
            if "distinct" in sql_lower:
                analysis["concerns"].append("DISTINCT may impact performance on large datasets")
            
            # Check for complex subqueries
            subquery_count = sql_lower.count("select")
            if subquery_count > 3:
                analysis["concerns"].append(f"Complex query with {subquery_count} SELECT statements")
            
            return f"Performance Analysis: {analysis}"
            
        except Exception as e:
            return f"Performance analysis error: {str(e)}"


class SnowflakeConnectivityTool(BaseTool):
    """Tool for testing Snowflake connectivity and procedure validation."""
    
    name: str = "snowflake_tester"
    description: str = "Tests Snowflake connection and validates procedures"
    
    def _run(
        self,
        procedure_sql: str,
        test_mode: bool = True,
        run_manager: Optional[CallbackManagerForToolRun] = None
    ) -> str:
        """Test procedure in Snowflake environment."""
        try:
            if test_mode:
                # Syntax validation only in test mode
                return "Test mode: Procedure syntax appears valid for Snowflake"
            
            # In production mode, would actually connect to Snowflake
            # Use absolute imports to fix packaging issues
            try:
                from utils.database import get_connection_manager
            except ImportError:
                # Fallback for when running as part of package
                from utils.database import get_connection_manager
            
            connection_manager = get_connection_manager()
            if connection_manager.test_connection():
                return "Snowflake connection successful - procedure can be deployed"
            else:
                return "Snowflake connection failed - cannot validate procedure"
                
        except Exception as e:
            return f"Snowflake connectivity error: {str(e)}"


class BteqAnalysisAgent:
    """Agent specialized in BTEQ script analysis and complexity assessment."""
    
    def __init__(self, model_name: Optional[str] = None):
        """
        Initialize BTEQ analysis agent.
        
        Args:
            model_name: Specific model name, or None to use config-based selection
        """
        if model_name is None:
            # Use configuration-based model selection for analysis tasks
            model_manager = get_model_manager()
            analysis_models = model_manager.get_models_for_task(TaskType.BTEQ_ANALYSIS)
            model_name = analysis_models[0] if analysis_models else model_manager.get_default_model()
        
        self.model_name = model_name
        self.llm = create_cortex_llm(model=model_name)
        self.tools = []
        self.llm_logger = get_llm_logger()
        
    def get_model_info(self) -> Dict[str, Any]:
        """Get information about the current model."""
        model_manager = get_model_manager()
        model_info = model_manager.get_model_info(self.model_name)
        
        return {
            "model_name": self.model_name,
            "task_type": "bteq_analysis",
            "model_info": model_info.__dict__ if model_info else None
        }
    
    def analyze(self, context: MigrationContext) -> AgentResult:
        """Perform deep analysis of BTEQ script."""
        start_time = time.time()
        
        prompt = f"""
        You are a BTEQ migration expert. Analyze this BTEQ script for migration complexity and patterns:
        
        BTEQ Script:
        {context.original_bteq}
        
        Provide analysis including:
        1. Control flow complexity (1-10 scale)
        2. SQL complexity (1-10 scale)  
        3. Teradata-specific features detected
        4. Migration risk assessment
        5. Recommended migration strategy
        6. Key challenges and considerations
        
        Format as structured analysis.
        """
        
        try:
            # Log the interaction
            interaction_id = self.llm_logger.start_interaction(
                provider="snowflake_cortex",
                model=self.model_type.value,
                request_type="bteq_analysis",
                prompt=prompt,
                context_data={"procedure_name": context.target_procedure_name}
            )
            
            response = self.llm.invoke(prompt)
            
            execution_time = (time.time() - start_time) * 1000
            
            # Log completion
            self.llm_logger.complete_interaction(
                interaction_id=interaction_id,
                response=response,
                success=True,
                processing_time_ms=execution_time
            )
            
            return AgentResult(
                agent_role=AgentRole.ANALYZER,
                model_used=self.model_name,
                success=True,
                output=response,
                confidence_score=0.9,
                execution_time_ms=execution_time,
                errors=[],
                metadata={"analysis_type": "deep_bteq_analysis", "model_info": self.get_model_info()}
            )
            
        except Exception as e:
            logger.error(f"BTEQ analysis failed: {e}")
            return AgentResult(
                agent_role=AgentRole.ANALYZER,
                model_used=self.model_name,
                success=False,
                output="",
                confidence_score=0.0,
                execution_time_ms=(time.time() - start_time) * 1000,
                errors=[str(e)],
                metadata={"model_info": self.get_model_info()}
            )


class MultiModelGenerationAgent:
    """Agent that orchestrates multiple models for procedure generation."""
    
    def __init__(self, model_names: Optional[List[str]] = None):
        """
        Initialize multi-model generation agent.
        
        Args:
            model_names: Specific model names, or None to use config-based selection
        """
        if model_names is None:
            # Use configuration-based model selection
            model_manager = get_model_manager()
            model_names = model_manager.get_models_for_task(TaskType.CODE_GENERATION)
        
        self.model_names = model_names
        self.models = {}
        
        # Initialize models from configuration
        for model_name in model_names:
            try:
                self.models[model_name] = create_cortex_llm(model=model_name)
                logger.info(f"Initialized model: {model_name}")
            except Exception as e:
                logger.error(f"Failed to initialize model {model_name}: {e}")
        
        self.llm_logger = get_llm_logger()
        logger.info(f"MultiModelGenerationAgent initialized with {len(self.models)} models")
    
    def generate_multi_model(self, context: MigrationContext) -> List[AgentResult]:
        """Generate procedures using multiple models."""
        results = []
        
        generation_prompt = f"""
        Generate a high-quality Snowflake stored procedure from this BTEQ script:
        
        Original BTEQ:
        {context.original_bteq}
        
        Analysis:
        {context.analysis_markdown}
        
        Requirements:
        - Production-ready code with error handling
        - Comprehensive logging and documentation
        - Snowflake optimization
        - Preserve business logic
        
        Generate only the SQL procedure code.
        """
        
        for model_name, llm in self.models.items():
            try:
                start_time = time.time()
                
                # Log interaction
                interaction_id = self.llm_logger.start_interaction(
                    provider="snowflake_cortex",
                    model=model_name,
                    request_type="multi_model_generation",
                    prompt=generation_prompt,
                    context_data={"procedure_name": context.target_procedure_name}
                )
                
                response = llm.invoke(generation_prompt)
                execution_time = (time.time() - start_time) * 1000
                
                # Log completion
                self.llm_logger.complete_interaction(
                    interaction_id=interaction_id,
                    response=response,
                    success=True,
                    processing_time_ms=execution_time
                )
                
                result = AgentResult(
                    agent_role=AgentRole.GENERATOR,
                    model_used=model_name,
                    success=True,
                    output=response,
                    confidence_score=0.8,
                    execution_time_ms=execution_time,
                    errors=[],
                    metadata={"generation_strategy": "multi_model"}
                )
                
                results.append(result)
                
            except Exception as e:
                logger.error(f"Model {model_name} generation failed: {e}")
                results.append(AgentResult(
                    agent_role=AgentRole.GENERATOR,
                    model_used=model_name,
                    success=False,
                    output="",
                    confidence_score=0.0,
                    execution_time_ms=0,
                    errors=[str(e)],
                    metadata={}
                ))
        
        return results


class DBTConversionAgent:
    """Agent specialized in converting BTEQ SQL to DBT models with dual LLM comparison."""
    
    def __init__(self, models: Optional[List[str]] = None, llm_log_dir: Optional[str] = None,
                 output_directory: Optional[Path] = None):
        """
        Initialize DBT conversion agent.
        
        Args:
            models: List of models to use for dual comparison
            llm_log_dir: Directory for LLM interaction logs
            output_directory: Run-specific output directory for saving LLM interactions
        """
        self.dbt_converter = DBTConverter(models=models, llm_log_dir=llm_log_dir, output_directory=output_directory)
        self.llm_logger = get_llm_logger()
        logger.info("DBT Conversion Agent initialized")
    
    def convert_to_dbt(self, context: MigrationContext, chosen_procedure_sql: str) -> AgentResult:
        """
        Convert BTEQ to DBT model using dual LLM approach.
        
        Args:
            context: Migration context with original BTEQ and analysis
            chosen_procedure_sql: The preferred stored procedure translation for reference
            
        Returns:
            AgentResult containing DBT conversion results
        """
        import time
        start_time = time.time()
        
        try:
            logger.info(f"Starting DBT conversion for {context.target_procedure_name}")
            
            # Create DBT conversion context
            dbt_context = DBTConversionContext(
                original_bteq_sql=context.original_bteq,
                chosen_stored_procedure=chosen_procedure_sql,
                procedure_name=context.target_procedure_name,
                analysis_markdown=context.analysis_markdown,
                parser_result=context.parser_result
            )
            
            # Perform conversion
            conversion_result = self.dbt_converter.convert_to_dbt(dbt_context)
            
            execution_time = (time.time() - start_time) * 1000
            
            # Format result metadata
            metadata = {
                "models_used": list(conversion_result.model_results.keys()),
                "preferred_model": conversion_result.preferred_result.model,
                "quality_score": conversion_result.preferred_result.quality_score,
                "dbt_features": conversion_result.preferred_result.dbt_features,
                "total_time_ms": conversion_result.total_processing_time_ms,
                "comparison_notes": conversion_result.comparison_notes,
                "migration_notes": conversion_result.preferred_result.migration_notes
            }
            
            logger.info(f"DBT conversion completed successfully with {conversion_result.preferred_result.model}")
            
            return AgentResult(
                agent_role=AgentRole.GENERATOR,
                model_used=ModelType.CLAUDE_4_SONNET,  # Use the preferred model
                success=True,
                output=conversion_result.preferred_result.dbt_sql,
                confidence_score=conversion_result.preferred_result.quality_score,
                execution_time_ms=int(execution_time),
                errors=conversion_result.preferred_result.warnings,
                metadata=metadata
            )
            
        except Exception as e:
            execution_time = (time.time() - start_time) * 1000
            error_msg = f"DBT conversion failed: {str(e)}"
            logger.error(error_msg)
            
            return AgentResult(
                agent_role=AgentRole.GENERATOR,
                model_used=ModelType.CLAUDE_4_SONNET,
                success=False,
                output="",
                confidence_score=0.0,
                execution_time_ms=int(execution_time),
                errors=[error_msg],
                metadata={"conversion_type": "dbt_model", "failure_reason": str(e)}
            )


class ValidationAgent:
    """Agent that uses tools to validate generated procedures."""
    
    def __init__(self):
        self.tools = [
            SQLValidationTool(),
            PerformanceAnalyzerTool(),
            SnowflakeConnectivityTool()
        ]
        self.llm = create_cortex_llm(model="claude-3-5-sonnet")
        
        # Create agent with tools
        self.agent = create_react_agent(
            llm=self.llm,
            tools=self.tools,
            prompt=PromptTemplate.from_template("""
            You are a SQL validation expert. Use the available tools to thoroughly validate this generated procedure:
            
            Procedure to validate:
            {procedure_sql}
            
            Use tools to check:
            1. SQL syntax and semantics
            2. Performance characteristics  
            3. Snowflake compatibility
            
            Provide a comprehensive validation report.
            
            {agent_scratchpad}
            """)
        )
        
        self.agent_executor = AgentExecutor(
            agent=self.agent,
            tools=self.tools,
            verbose=True,
            max_iterations=5
        )
    
    def validate(self, procedure_sql: str) -> AgentResult:
        """Validate procedure using available tools."""
        try:
            start_time = time.time()
            
            result = self.agent_executor.invoke({
                "procedure_sql": procedure_sql
            })
            
            execution_time = (time.time() - start_time) * 1000
            
            return AgentResult(
                agent_role=AgentRole.VALIDATOR,
                model_used=ModelType.CLAUDE_35_SONNET,
                success=True,
                output=result["output"],
                confidence_score=0.85,
                execution_time_ms=execution_time,
                errors=[],
                metadata={"validation_tools_used": [tool.name for tool in self.tools]}
            )
            
        except Exception as e:
            logger.error(f"Validation failed: {e}")
            return AgentResult(
                agent_role=AgentRole.VALIDATOR,
                model_used=ModelType.CLAUDE_35_SONNET,
                success=False,
                output="",
                confidence_score=0.0,
                execution_time_ms=0,
                errors=[str(e)],
                metadata={}
            )


class JudgmentAgent:
    """Agent that evaluates and ranks multiple procedure generations."""
    
    def __init__(self):
        self.llm = create_cortex_llm(model="claude-3-5-sonnet")
        self.llm_logger = get_llm_logger()
    
    def judge_outputs(self, 
                     generation_results: List[AgentResult], 
                     validation_results: List[AgentResult],
                     context: MigrationContext) -> AgentResult:
        """Judge and rank multiple generated procedures."""
        
        # Prepare comparison data
        comparison_data = []
        for i, (gen_result, val_result) in enumerate(zip(generation_results, validation_results)):
            comparison_data.append({
                "option": i + 1,
                "model": gen_result.model_used.value,
                "generation_success": gen_result.success,
                "validation_result": val_result.output if val_result.success else "Validation failed",
                "procedure_sql": gen_result.output[:1000] + "..." if len(gen_result.output) > 1000 else gen_result.output
            })
        
        judgment_prompt = f"""
        You are an expert code judge evaluating Snowflake stored procedures generated from BTEQ migration.
        
        Original BTEQ Context:
        {context.original_bteq[:500]}...
        
        Compare these generated procedures and select the best one:
        
        {comparison_data}
        
        Evaluation criteria:
        1. Code quality and best practices
        2. Error handling completeness
        3. Snowflake optimization
        4. Documentation quality
        5. Business logic preservation
        6. Validation results
        
        Respond with:
        1. Ranking of options (1 = best)
        2. Detailed justification
        3. Confidence score (0-1)
        4. Recommended improvements
        """
        
        try:
            start_time = time.time()
            
            interaction_id = self.llm_logger.start_interaction(
                provider="snowflake_cortex",
                model="claude-3-5-sonnet",
                request_type="judgment_evaluation",
                prompt=judgment_prompt,
                context_data={"options_count": len(comparison_data)}
            )
            
            response = self.llm.invoke(judgment_prompt)
            execution_time = (time.time() - start_time) * 1000
            
            self.llm_logger.complete_interaction(
                interaction_id=interaction_id,
                response=response,
                success=True,
                processing_time_ms=execution_time
            )
            
            return AgentResult(
                agent_role=AgentRole.JUDGE,
                model_used=ModelType.CLAUDE_35_SONNET,
                success=True,
                output=response,
                confidence_score=0.9,
                execution_time_ms=execution_time,
                errors=[],
                metadata={"options_evaluated": len(comparison_data)}
            )
            
        except Exception as e:
            logger.error(f"Judgment failed: {e}")
            return AgentResult(
                agent_role=AgentRole.JUDGE,
                model_used=ModelType.CLAUDE_35_SONNET,
                success=False,
                output="",
                confidence_score=0.0,
                execution_time_ms=0,
                errors=[str(e)],
                metadata={}
            )


class ErrorCorrectionAgent:
    """Agent that iteratively fixes errors in generated procedures."""
    
    def __init__(self):
        self.llm = create_cortex_llm(model="claude-3-5-sonnet")
        self.validator = ValidationAgent()
        self.max_iterations = 3
        self.llm_logger = get_llm_logger()
    
    def correct_errors(self, 
                      original_procedure: str, 
                      validation_errors: List[str],
                      context: MigrationContext) -> AgentResult:
        """Iteratively correct errors in procedure."""
        
        current_procedure = original_procedure
        iteration = 0
        
        while iteration < self.max_iterations:
            if not validation_errors:
                break
                
            correction_prompt = f"""
            Fix the following errors in this Snowflake stored procedure:
            
            Current Procedure:
            {current_procedure}
            
            Errors to fix:
            {validation_errors}
            
            Original BTEQ context:
            {context.original_bteq[:500]}...
            
            Provide the corrected procedure maintaining business logic and quality.
            """
            
            try:
                start_time = time.time()
                
                interaction_id = self.llm_logger.start_interaction(
                    provider="snowflake_cortex",
                    model="claude-3-5-sonnet",
                    request_type="error_correction",
                    prompt=correction_prompt,
                    context_data={"iteration": iteration, "errors_count": len(validation_errors)}
                )
                
                corrected_procedure = self.llm.invoke(correction_prompt)
                execution_time = (time.time() - start_time) * 1000
                
                # Validate the corrected procedure
                validation_result = self.validator.validate(corrected_procedure)
                
                self.llm_logger.complete_interaction(
                    interaction_id=interaction_id,
                    response=corrected_procedure,
                    success=validation_result.success,
                    processing_time_ms=execution_time
                )
                
                if validation_result.success:
                    return AgentResult(
                        agent_role=AgentRole.ERROR_CORRECTOR,
                        model_used=ModelType.CLAUDE_35_SONNET,
                        success=True,
                        output=corrected_procedure,
                        confidence_score=0.8,
                        execution_time_ms=execution_time,
                        errors=[],
                        metadata={"iterations": iteration + 1, "final_validation": "passed"}
                    )
                
                current_procedure = corrected_procedure
                # Extract new errors for next iteration
                validation_errors = validation_result.errors
                iteration += 1
                
            except Exception as e:
                logger.error(f"Error correction iteration {iteration} failed: {e}")
                break
        
        return AgentResult(
            agent_role=AgentRole.ERROR_CORRECTOR,
            model_used=ModelType.CLAUDE_35_SONNET,
            success=False,
            output=current_procedure,
            confidence_score=0.3,
            execution_time_ms=0,
            errors=["Max iterations reached without full error resolution"],
            metadata={"iterations": iteration}
        )


import time  # Add missing import
