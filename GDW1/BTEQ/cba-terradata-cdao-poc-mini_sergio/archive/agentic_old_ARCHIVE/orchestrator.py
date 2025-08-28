"""
LangGraph-based Orchestrator for Agentic BTEQ Migration

Uses LangGraph to coordinate multi-agent workflows with state management,
conditional routing, and error recovery.
"""

import logging
from typing import Dict, List, Any, Optional, TypedDict, Annotated
from dataclasses import dataclass, asdict
import json
import time

from langchain_core.messages import BaseMessage
from langgraph.graph import StateGraph, END
from langgraph.checkpoint.memory import MemorySaver
from langgraph.prebuilt import ToolNode

from .agents import (
    BteqAnalysisAgent, MultiModelGenerationAgent, ValidationAgent,
    JudgmentAgent, ErrorCorrectionAgent, DBTConversionAgent, AgentResult, MigrationContext,
    ModelType, AgentRole
)
# Use current project structure imports
from services.parsing.bteq.tokens import ParserResult
from utils.logging.llm_logger import get_llm_logger

logger = logging.getLogger(__name__)


class MigrationState(TypedDict):
    """State maintained throughout the migration workflow."""
    # Input data
    original_bteq: str
    parser_result: Dict[str, Any]  # Serialized ParserResult
    target_procedure_name: str
    
    # Workflow state
    current_step: str
    iteration_count: int
    max_iterations: int
    
    # Agent results
    analysis_result: Optional[Dict[str, Any]]
    generation_results: List[Dict[str, Any]]
    validation_results: List[Dict[str, Any]]
    judgment_result: Optional[Dict[str, Any]]
    correction_result: Optional[Dict[str, Any]]
    
    # Final outputs
    best_procedure: Optional[str]
    quality_score: float
    final_errors: List[str]
    workflow_metadata: Dict[str, Any]
    
    # Control flags
    needs_correction: bool
    validation_passed: bool
    workflow_complete: bool
    
    # DBT conversion results
    dbt_conversion_result: Optional[Dict[str, Any]]
    dbt_model_sql: Optional[str]


@dataclass
class OrchestrationConfig:
    """Configuration for the agentic orchestration."""
    max_iterations: int = 3
    enable_multi_model: bool = True
    enable_error_correction: bool = True
    enable_tool_validation: bool = True
    enable_dbt_conversion: bool = False  # New DBT conversion option
    quality_threshold: float = 0.8
    models_to_use: List[ModelType] = None
    dbt_models_to_use: Optional[List[str]] = None  # Specific models for DBT conversion
    
    def __post_init__(self):
        if self.models_to_use is None:
            self.models_to_use = [ModelType.CLAUDE_4_SONNET, ModelType.SNOWFLAKE_LLAMA_33_70B]
        if self.dbt_models_to_use is None:
            self.dbt_models_to_use = ["claude-4-sonnet", "snowflake-llama-3.3-70b"]


class AgenticOrchestrator:
    """LangGraph-based orchestrator for agentic BTEQ migration."""
    
    def __init__(self, config: OrchestrationConfig = None):
        self.config = config or OrchestrationConfig()
        self.llm_logger = get_llm_logger()
        
        # Initialize agents
        self.analysis_agent = BteqAnalysisAgent()
        self.generation_agent = MultiModelGenerationAgent()
        self.validation_agent = ValidationAgent()
        self.judgment_agent = JudgmentAgent()
        self.correction_agent = ErrorCorrectionAgent()
        
        # Initialize DBT conversion agent if enabled
        if self.config.enable_dbt_conversion:
            self.dbt_agent = DBTConversionAgent(
                models=self.config.dbt_models_to_use,
                llm_log_dir=None  # Uses default from LLM logger
            )
        else:
            self.dbt_agent = None
        
        # Build the workflow graph
        self.workflow = self._build_workflow()
        
        # Initialize memory for state persistence
        self.memory = MemorySaver()
        self.app = self.workflow.compile(checkpointer=self.memory)
    
    def _build_workflow(self) -> StateGraph:
        """Build the LangGraph workflow for agentic migration."""
        
        workflow = StateGraph(MigrationState)
        
        # Add workflow nodes
        workflow.add_node("analyze", self._analyze_node)
        workflow.add_node("generate", self._generate_node)
        workflow.add_node("validate", self._validate_node)
        workflow.add_node("judge", self._judge_node)
        workflow.add_node("correct", self._correct_node)
        workflow.add_node("finalize", self._finalize_node)
        
        # Define workflow edges and conditions
        workflow.set_entry_point("analyze")
        
        workflow.add_edge("analyze", "generate")
        workflow.add_edge("generate", "validate")
        workflow.add_edge("validate", "judge")
        
        # Conditional routing after judgment
        workflow.add_conditional_edges(
            "judge",
            self._should_correct,
            {
                "correct": "correct",
                "finalize": "finalize"
            }
        )
        
        # Conditional routing after correction
        workflow.add_conditional_edges(
            "correct",
            self._should_retry,
            {
                "validate": "validate",
                "finalize": "finalize"
            }
        )
        
        workflow.add_edge("finalize", END)
        
        return workflow
    
    def _analyze_node(self, state: MigrationState) -> MigrationState:
        """Analysis node - deep BTEQ analysis."""
        logger.info("Executing analysis node")
        
        # Reconstruct context from state
        parser_result = ParserResult(**state["parser_result"])
        context = MigrationContext(
            original_bteq=state["original_bteq"],
            parser_result=parser_result,
            analysis_markdown="",  # Will be populated
            complexity_score=5.0,  # Default
            target_procedure_name=state["target_procedure_name"],
            quality_requirements={},
            validation_rules=[]
        )
        
        # Execute analysis
        analysis_result = self.analysis_agent.analyze(context)
        
        # Update state
        state["analysis_result"] = asdict(analysis_result)
        state["current_step"] = "analyze"
        state["workflow_metadata"]["analysis_completed"] = True
        
        return state
    
    def _generate_node(self, state: MigrationState) -> MigrationState:
        """Generation node - multi-model procedure generation."""
        logger.info("Executing generation node")
        
        # Reconstruct context
        parser_result = ParserResult(**state["parser_result"])
        analysis_output = state["analysis_result"]["output"] if state["analysis_result"] else ""
        
        context = MigrationContext(
            original_bteq=state["original_bteq"],
            parser_result=parser_result,
            analysis_markdown=analysis_output,
            complexity_score=5.0,
            target_procedure_name=state["target_procedure_name"],
            quality_requirements={},
            validation_rules=[]
        )
        
        # Execute multi-model generation
        generation_results = self.generation_agent.generate_multi_model(context)
        
        # Update state
        state["generation_results"] = [asdict(result) for result in generation_results]
        state["current_step"] = "generate"
        state["workflow_metadata"]["generation_completed"] = True
        
        return state
    
    def _validate_node(self, state: MigrationState) -> MigrationState:
        """Validation node - tool-based validation."""
        logger.info("Executing validation node")
        
        validation_results = []
        
        # Validate each generated procedure
        for gen_result_dict in state["generation_results"]:
            if gen_result_dict["success"]:
                procedure_sql = gen_result_dict["output"]
                validation_result = self.validation_agent.validate(procedure_sql)
                validation_results.append(asdict(validation_result))
            else:
                # Create failed validation for failed generation
                validation_results.append({
                    "agent_role": AgentRole.VALIDATOR.value,
                    "model_used": gen_result_dict["model_used"],
                    "success": False,
                    "output": "Generation failed - cannot validate",
                    "confidence_score": 0.0,
                    "execution_time_ms": 0,
                    "errors": ["Generation failure"],
                    "metadata": {}
                })
        
        # Update state
        state["validation_results"] = validation_results
        state["current_step"] = "validate"
        state["workflow_metadata"]["validation_completed"] = True
        
        # Check if any validation passed
        state["validation_passed"] = any(result["success"] for result in validation_results)
        
        return state
    
    def _judge_node(self, state: MigrationState) -> MigrationState:
        """Judgment node - evaluate and rank generated procedures."""
        logger.info("Executing judgment node")
        
        # Reconstruct agent results
        generation_results = [AgentResult(**result) for result in state["generation_results"]]
        validation_results = [AgentResult(**result) for result in state["validation_results"]]
        
        # Reconstruct context
        parser_result = ParserResult(**state["parser_result"])
        context = MigrationContext(
            original_bteq=state["original_bteq"],
            parser_result=parser_result,
            analysis_markdown=state["analysis_result"]["output"] if state["analysis_result"] else "",
            complexity_score=5.0,
            target_procedure_name=state["target_procedure_name"],
            quality_requirements={},
            validation_rules=[]
        )
        
        # Execute judgment
        judgment_result = self.judgment_agent.judge_outputs(
            generation_results, validation_results, context
        )
        
        # Extract best procedure and quality score
        best_procedure = None
        quality_score = 0.0
        
        # Find the best procedure based on judgment
        if judgment_result.success and generation_results:
            # For now, use the first successful generation
            for gen_result in generation_results:
                if gen_result.success:
                    best_procedure = gen_result.output
                    quality_score = gen_result.confidence_score
                    break
        
        # Update state
        state["judgment_result"] = asdict(judgment_result)
        state["best_procedure"] = best_procedure
        state["quality_score"] = quality_score
        state["current_step"] = "judge"
        state["workflow_metadata"]["judgment_completed"] = True
        
        # Determine if correction is needed
        state["needs_correction"] = (
            quality_score < self.config.quality_threshold or 
            not state["validation_passed"]
        )
        
        return state
    
    def _correct_node(self, state: MigrationState) -> MigrationState:
        """Correction node - iterative error correction."""
        logger.info("Executing correction node")
        
        if not state["best_procedure"]:
            logger.warning("No procedure to correct")
            state["correction_result"] = {
                "success": False,
                "errors": ["No procedure available for correction"]
            }
            return state
        
        # Extract validation errors
        validation_errors = []
        for val_result in state["validation_results"]:
            if not val_result["success"]:
                validation_errors.extend(val_result["errors"])
        
        # Reconstruct context
        parser_result = ParserResult(**state["parser_result"])
        context = MigrationContext(
            original_bteq=state["original_bteq"],
            parser_result=parser_result,
            analysis_markdown=state["analysis_result"]["output"] if state["analysis_result"] else "",
            complexity_score=5.0,
            target_procedure_name=state["target_procedure_name"],
            quality_requirements={},
            validation_rules=[]
        )
        
        # Execute correction
        correction_result = self.correction_agent.correct_errors(
            state["best_procedure"], validation_errors, context
        )
        
        # Update state
        state["correction_result"] = asdict(correction_result)
        state["current_step"] = "correct"
        state["iteration_count"] += 1
        
        if correction_result.success:
            state["best_procedure"] = correction_result.output
            state["quality_score"] = correction_result.confidence_score
            state["needs_correction"] = False
        
        return state
    
    def _finalize_node(self, state: MigrationState) -> MigrationState:
        """Finalization node - prepare final output."""
        logger.info("Executing finalization node")
        
        # Collect final errors
        final_errors = []
        if state["correction_result"] and not state["correction_result"]["success"]:
            final_errors.extend(state["correction_result"]["errors"])
        
        # Update final state
        state["final_errors"] = final_errors
        state["workflow_complete"] = True
        state["current_step"] = "finalize"
        state["workflow_metadata"]["finalize_completed"] = True
        state["workflow_metadata"]["total_execution_time"] = time.time() - state["workflow_metadata"]["start_time"]
        
        logger.info(f"Workflow completed. Quality score: {state['quality_score']}")
        
        return state
    
    def _should_correct(self, state: MigrationState) -> str:
        """Determine if error correction is needed."""
        if (state["needs_correction"] and 
            self.config.enable_error_correction and 
            state["iteration_count"] < self.config.max_iterations):
            return "correct"
        return "finalize"
    
    def _should_retry(self, state: MigrationState) -> str:
        """Determine if another validation round is needed."""
        if (state["correction_result"] and 
            state["correction_result"]["success"] and
            state["iteration_count"] < self.config.max_iterations):
            return "validate"
        return "finalize"
    
    def migrate_bteq(self, 
                     original_bteq: str, 
                     parser_result: ParserResult,
                     target_procedure_name: str) -> Dict[str, Any]:
        """
        Execute the complete agentic migration workflow.
        
        Args:
            original_bteq: Original BTEQ script content
            parser_result: Parsed BTEQ structure
            target_procedure_name: Name for generated procedure
            
        Returns:
            Migration results with final procedure and metadata
        """
        logger.info(f"Starting agentic migration for {target_procedure_name}")
        
        # Initialize state
        initial_state = MigrationState(
            original_bteq=original_bteq,
            parser_result=asdict(parser_result),
            target_procedure_name=target_procedure_name,
            current_step="start",
            iteration_count=0,
            max_iterations=self.config.max_iterations,
            analysis_result=None,
            generation_results=[],
            validation_results=[],
            judgment_result=None,
            correction_result=None,
            best_procedure=None,
            quality_score=0.0,
            final_errors=[],
            workflow_metadata={
                "start_time": time.time(),
                "config": asdict(self.config)
            },
            needs_correction=False,
            validation_passed=False,
            workflow_complete=False
        )
        
        try:
            # Execute the workflow
            thread_config = {"configurable": {"thread_id": f"migration_{int(time.time())}"}}
            final_state = self.app.invoke(initial_state, config=thread_config)
            
            # Prepare final results
            results = {
                "success": final_state["workflow_complete"],
                "procedure_name": target_procedure_name,
                "generated_procedure": final_state["best_procedure"],
                "quality_score": final_state["quality_score"],
                "iterations": final_state["iteration_count"],
                "errors": final_state["final_errors"],
                "metadata": {
                    "workflow_metadata": final_state["workflow_metadata"],
                    "analysis_result": final_state["analysis_result"],
                    "generation_count": len(final_state["generation_results"]),
                    "validation_passed": final_state["validation_passed"],
                    "correction_applied": final_state["correction_result"] is not None
                }
            }
            
            # Log summary
            self.llm_logger.log_simple_interaction(
                provider="agentic_orchestrator",
                model="multi_model",
                request_type="complete_migration",
                prompt=f"BTEQ migration for {target_procedure_name}",
                response=f"Quality: {final_state['quality_score']}, Iterations: {final_state['iteration_count']}",
                success=final_state["workflow_complete"],
                processing_time_ms=final_state["workflow_metadata"].get("total_execution_time", 0) * 1000,
                context_data={"procedure_name": target_procedure_name}
            )
            
            return results
            
        except Exception as e:
            logger.error(f"Agentic migration failed: {e}")
            return {
                "success": False,
                "procedure_name": target_procedure_name,
                "generated_procedure": None,
                "quality_score": 0.0,
                "iterations": 0,
                "errors": [str(e)],
                "metadata": {"error": "Workflow execution failed"}
            }
    
    def get_workflow_status(self, thread_id: str) -> Dict[str, Any]:
        """Get the current status of a workflow."""
        try:
            # Get state from memory
            thread_config = {"configurable": {"thread_id": thread_id}}
            state = self.app.get_state(thread_config)
            
            if state and state.values:
                return {
                    "current_step": state.values.get("current_step", "unknown"),
                    "iteration_count": state.values.get("iteration_count", 0),
                    "quality_score": state.values.get("quality_score", 0.0),
                    "workflow_complete": state.values.get("workflow_complete", False),
                    "validation_passed": state.values.get("validation_passed", False)
                }
            else:
                return {"error": "Workflow not found"}
                
        except Exception as e:
            return {"error": f"Status check failed: {str(e)}"}


# Factory function for easy usage
def create_agentic_orchestrator(config: OrchestrationConfig = None) -> AgenticOrchestrator:
    """Create and return an agentic orchestrator instance."""
    return AgenticOrchestrator(config)
