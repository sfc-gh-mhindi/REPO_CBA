"""
DBT Integration for Agentic BTEQ Migration

Integrates DBT conversion capabilities into the existing agentic migration pipeline.
Provides simple API for enabling DBT conversion alongside stored procedure generation.
"""

import logging
from dataclasses import dataclass
from typing import Optional, Dict, Any, List
from pathlib import Path

# Use current project structure imports
from ..agents.service import DBTConversionAgent, MigrationContext, AgentResult
from services.conversion.dbt.converter import DBTConversionContext, DBTConversionResult
from utils.logging.llm_logger import get_llm_logger

logger = logging.getLogger(__name__)


@dataclass
class DBTMigrationResult:
    """Complete result of BTEQ migration with DBT conversion."""
    stored_procedure_result: Optional[Dict[str, Any]]
    dbt_conversion_result: Optional[DBTConversionResult] 
    preferred_stored_procedure_sql: Optional[str]
    dbt_model_sql: Optional[str]
    migration_summary: Dict[str, Any]


class DBTEnabledMigrationPipeline:
    """
    Enhanced migration pipeline with DBT conversion capabilities.
    
    Extends the existing agentic migration to include DBT model generation
    alongside stored procedure generation.
    """
    
    def __init__(self, 
                 enable_dbt: bool = True,
                 dbt_models: Optional[List[str]] = None,
                 output_directory: Optional[str] = None,
                 dry_run: bool = False):
        """
        Initialize DBT-enabled migration pipeline.
        
        Args:
            enable_dbt: Whether to enable DBT conversion
            dbt_models: List of models for DBT conversion
            output_directory: Base output directory
        """
        self.enable_dbt = enable_dbt
        self.output_directory = Path(output_directory) if output_directory else None
        self.llm_logger = get_llm_logger()
        
        # Initialize DBT conversion agent if enabled
        if enable_dbt:
            self.dbt_agent = DBTConversionAgent(
                models=dbt_models or ["claude-4-sonnet", "snowflake-llama-3.3-70b"],
                output_directory=self.output_directory if self.output_directory else None  # Pass run-specific main directory
            )
            logger.info("DBT conversion enabled in migration pipeline")
        else:
            self.dbt_agent = None
            logger.info("DBT conversion disabled")
    
    def migrate_with_dbt(self, 
                        original_bteq: str,
                        procedure_name: str,
                        stored_procedure_sql: Optional[str] = None,
                        analysis_markdown: Optional[str] = None) -> DBTMigrationResult:
        """
        Perform migration with optional DBT conversion.
        
        Args:
            original_bteq: Original BTEQ script content
            procedure_name: Target procedure name
            stored_procedure_sql: Pre-generated stored procedure (required for DBT)
            analysis_markdown: Optional analysis context
            
        Returns:
            Complete migration result with both SP and DBT outputs
        """
        logger.info(f"Starting enhanced migration for {procedure_name} (DBT enabled: {self.enable_dbt})")
        
        dbt_result = None
        dbt_sql = None
        
        # Perform DBT conversion if enabled and stored procedure is available
        if self.enable_dbt and self.dbt_agent and stored_procedure_sql:
            try:
                # Create migration context for DBT conversion with all required fields
                from services.parsing.bteq.tokens import ParserResult
                
                # Create a minimal parser result if none provided  
                parser_result = ParserResult(
                    controls=[],
                    sql_blocks=[]
                )
                
                migration_context = MigrationContext(
                    original_bteq=original_bteq,
                    parser_result=parser_result,
                    analysis_markdown=analysis_markdown or f"BTEQ to DBT conversion for {procedure_name}",
                    complexity_score=0.7,  # Default medium complexity
                    target_procedure_name=procedure_name,
                    quality_requirements={"dbt_best_practices": True},
                    validation_rules=["no_hallucination", "preserve_logic"]
                )
                
                # Convert to DBT
                dbt_agent_result = self.dbt_agent.convert_to_dbt(
                    migration_context, 
                    stored_procedure_sql
                )
                
                if dbt_agent_result.output:
                    dbt_sql = dbt_agent_result.output
                    logger.info(f"✅ DBT conversion completed for {procedure_name}")
                else:
                    logger.warning(f"⚠️ DBT conversion returned empty result for {procedure_name}")
                
                # Extract DBT conversion details from metadata
                dbt_result = {
                    "success": len(dbt_agent_result.errors) == 0,
                    "quality_score": dbt_agent_result.confidence_score,
                    "model_sql": dbt_sql,
                    "metadata": dbt_agent_result.metadata,
                    "warnings": dbt_agent_result.errors,
                    "execution_time_ms": dbt_agent_result.execution_time_ms
                }
                
            except Exception as e:
                logger.error(f"❌ DBT conversion failed for {procedure_name}: {e}")
                dbt_result = {
                    "success": False,
                    "error": str(e),
                    "model_sql": None
                }
        elif self.enable_dbt and not stored_procedure_sql:
            logger.warning("DBT conversion enabled but no stored procedure provided")
        
        # Create migration summary
        summary = {
            "procedure_name": procedure_name,
            "dbt_enabled": self.enable_dbt,
            "dbt_conversion_success": dbt_result.get("success", False) if dbt_result else False,
            "has_stored_procedure": stored_procedure_sql is not None,
            "has_dbt_model": dbt_sql is not None,
            "output_types": []
        }
        
        if stored_procedure_sql:
            summary["output_types"].append("stored_procedure")
        if dbt_sql:
            summary["output_types"].append("dbt_model")
        
        # Save outputs if output directory specified
        if self.output_directory:
            self._save_migration_outputs(
                procedure_name=procedure_name,
                stored_procedure_sql=stored_procedure_sql,
                dbt_sql=dbt_sql,
                dbt_result=dbt_result,
                summary=summary
            )
        
        return DBTMigrationResult(
            stored_procedure_result={"sql": stored_procedure_sql} if stored_procedure_sql else None,
            dbt_conversion_result=dbt_result,
            preferred_stored_procedure_sql=stored_procedure_sql,
            dbt_model_sql=dbt_sql,
            migration_summary=summary
        )
    
    def _save_migration_outputs(self,
                               procedure_name: str,
                               stored_procedure_sql: Optional[str],
                               dbt_sql: Optional[str],
                               dbt_result: Optional[Dict[str, Any]],
                               summary: Dict[str, Any]):
        """Save migration outputs to clean folder structure."""
        try:
            # Create clean output directory structure: results/dbt/
            results_dir = self.output_directory / "results" 
            dbt_dir = results_dir / "dbt"
            dbt_dir.mkdir(parents=True, exist_ok=True)
            
            # Use filename from LLM response if available, otherwise generate from procedure name
            if dbt_result and dbt_result.get('best_model_result') and dbt_result['best_model_result'].get('name'):
                dbt_model_name = f"{dbt_result['best_model_result']['name']}.sql"
            else:
                # Fallback: Create DBT model name (convert PROC_NAME to proc_name.sql)
                dbt_model_name = procedure_name.lower().replace('_isrt', '')
                dbt_model_name = f"{dbt_model_name}.sql"
            
            # Save DBT model if available
            if dbt_sql:
                dbt_path = dbt_dir / dbt_model_name
                dbt_path.write_text(dbt_sql)
                logger.info(f"Saved DBT model: {dbt_path}")
            
            # Also save both individual LLM outputs for comparison if available
            if dbt_result and 'all_model_results' in dbt_result:
                for model_result in dbt_result['all_model_results']:
                    model_name = model_result.get('model', 'unknown')
                    if model_result.get('dbt_sql'):
                        individual_path = dbt_dir / f"{dbt_model_name.replace('.sql', '')}_{model_name.replace('-', '_')}.sql"
                        individual_path.write_text(model_result['dbt_sql'])
                        logger.info(f"Saved individual {model_name} DBT model: {individual_path}")
            
            # Note: Reference exemplar SQL path can be logged in metadata if needed for traceability
            
            # Save metadata and summary in dbt folder
            if dbt_result:
                import json
                metadata_path = dbt_dir / f"{dbt_model_name.replace('.sql', '_metadata.json')}"
                metadata_path.write_text(json.dumps(dbt_result, indent=2))
                logger.info(f"Saved DBT metadata: {metadata_path}")
            
            # Save summary
            import json
            summary_path = dbt_dir / "migration_summary.json"
            summary_path.write_text(json.dumps(summary, indent=2))
            logger.info(f"Saved migration summary: {summary_path}")
            
        except Exception as e:
            logger.error(f"Failed to save migration outputs: {e}")


def create_dbt_enabled_pipeline(bteq_content: str,
                              procedure_name: str,
                              output_dir: str,
                              config: str = None,
                              llm_models: List[str] = None,
                              dry_run: bool = False) -> 'DBTEnabledMigrationPipeline':
    """
    Create a DBT-enabled migration pipeline with default settings.
    
    Args:
        bteq_content: Original BTEQ content  
        procedure_name: Name of the procedure
        output_dir: Output directory for results
        config: Configuration file path
        llm_models: List of models to use for DBT conversion
        dry_run: Whether to run in dry-run mode (generate prompts only)
        
    Returns:
        Configured DBT migration pipeline
    """
    return DBTEnabledMigrationPipeline(
        enable_dbt=True,
        dbt_models=llm_models or ["claude-4-sonnet"],
        output_directory=output_dir,
        dry_run=dry_run
    )


# Convenience function for quick DBT conversion
def convert_bteq_to_dbt(original_bteq_sql: str,
                       stored_procedure_sql: str,
                       procedure_name: str,
                       output_dir: Optional[str] = None) -> Optional[str]:
    """
    Quick utility to convert BTEQ to DBT model.
    
    Args:
        original_bteq_sql: Original BTEQ script
        stored_procedure_sql: Generated stored procedure for reference
        procedure_name: Target procedure name
        output_dir: Optional output directory
        
    Returns:
        Generated DBT model SQL or None if failed
    """
    pipeline = create_dbt_enabled_pipeline(output_dir)
    
    result = pipeline.migrate_with_dbt(
        original_bteq=original_bteq_sql,
        procedure_name=procedure_name,
        stored_procedure_sql=stored_procedure_sql
    )
    
    return result.dbt_model_sql
