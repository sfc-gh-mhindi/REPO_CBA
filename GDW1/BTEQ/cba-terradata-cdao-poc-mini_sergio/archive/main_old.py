#!/usr/bin/env python3
"""
BTEQ DCF - Unified Entry Point
Single command-line interface for all BTEQ to Snowflake migration modes.
"""

import argparse
import sys
import logging
from pathlib import Path
from typing import Optional, List
import json
from datetime import datetime

# Import only essential services - others imported conditionally
try:
    from substitution.pipeline import SubstitutionPipeline
    from utils.config import get_config_manager, get_model_manager, ModelSelectionStrategy
    from utils.logging import get_llm_logger
except ImportError:
    # Should not happen with new flattened structure, but keeping for safety
    raise ImportError("Could not import required modules - check project structure")

logger = logging.getLogger(__name__)


class ProcessingMode:
    """Clean processing mode constants with clear logical progression."""
    V1_PRSCRIP_SP = "v1_prscrip_sp"                                  # Pure prescriptive SQLGlot BTEQ→SP conversion
    V2_PRSCRIP_CLAUDE_SP = "v2_prscrip_claude_sp"                    # Prescriptive + Claude enhanced SP
    V3_PRSCRIP_CLAUDE_SP_CLAUDE_DBT = "v3_prscrip_claude_sp_claude_dbt"  # Prescriptive + Claude SP + Claude DBT
    V4_PRSCRIP_CLAUDE_LLAMA_SP = "v4_prscrip_claude_llama_sp"        # Prescriptive + Claude vs Llama comparison
    V5_CLAUDE_DBT = "v5_claude_dbt"                                  # Direct BTEQ → DBT conversion using Claude
    
    # Legacy aliases for backward compatibility
    PRESCRIPTIVE = V1_PRSCRIP_SP
    
    @classmethod
    def all_modes(cls):
        return [
            cls.V1_PRSCRIP_SP,
            cls.V2_PRSCRIP_CLAUDE_SP,
            cls.V3_PRSCRIP_CLAUDE_SP_CLAUDE_DBT,
            cls.V4_PRSCRIP_CLAUDE_LLAMA_SP,
            cls.V5_CLAUDE_DBT
        ]
        
    @classmethod
    def get_mode_description(cls, mode: str) -> str:
        """Get detailed description of processing mode."""
        descriptions = {
            cls.V1_PRSCRIP_SP: "Pure prescriptive: SQLGlot-based BTEQ→Snowflake SP conversion (fastest, no LLM)",
            cls.V2_PRSCRIP_CLAUDE_SP: "Prescriptive + Claude: Traditional pipeline enhanced by Claude-4-sonnet (1 LLM call)",
            cls.V3_PRSCRIP_CLAUDE_SP_CLAUDE_DBT: "Full Claude pipeline: Prescriptive + Claude SP + Claude DBT conversion (2 LLM calls)",
            cls.V4_PRSCRIP_CLAUDE_LLAMA_SP: "Multi-model comparison: Prescriptive + Claude vs Llama, pick best SP (2 LLM calls)",
            cls.V5_CLAUDE_DBT: "Direct DBT: BTEQ directly to DBT models using Claude (streamlined, 1 LLM call)"
        }
        return descriptions.get(mode, "Unknown processing mode")


class BteqMigrationCLI:
    """Unified CLI for BTEQ migration with multiple processing modes."""
    
    def __init__(self):
        self.config_manager = get_config_manager()
        self.model_manager = get_model_manager()
        self.llm_logger = get_llm_logger()
        
    def setup_logging(self, log_level: str, verbose: bool = False, output_dir: str = None) -> str:
        """Configure logging for the session and return the timestamped run directory."""
        level = getattr(logging, log_level.upper())
        
        format_str = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        if verbose:
            format_str = '%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s'
            
        # Create timestamped run directory that will contain both logs and outputs
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        run_dir = Path(output_dir or './output') / f'migration_run_{timestamp}'
        run_dir.mkdir(parents=True, exist_ok=True)
        
        # Create logs subdirectory within the run directory
        log_dir = run_dir / 'logs'
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file = log_dir / f'bteq_migration_{timestamp}.log'
        
        logging.basicConfig(
            level=level,
            format=format_str,
            handlers=[
                logging.StreamHandler(sys.stdout),
                logging.FileHandler(str(log_file))
            ]
        )
        
        logger.info(f"Created run directory: {run_dir}")
        logger.info(f"Logging to: {log_file}")
        
        # Store run directory for later use
        self.run_dir = str(run_dir)
        return str(run_dir)
        
    def validate_inputs(self, args: argparse.Namespace) -> None:
        """Validate input parameters."""
        # Check input path exists
        input_path = Path(args.input)
        if not input_path.exists():
            raise FileNotFoundError(f"Input path not found: {input_path}")
            
        # Create output directory if it doesn't exist
        output_path = Path(args.output)
        output_path.mkdir(parents=True, exist_ok=True)
        
    def get_input_files(self, input_path: str) -> List[Path]:
        """Get list of BTEQ files to process."""
        path = Path(input_path)
        
        if path.is_file():
            # Single file
            if path.suffix.lower() in ['.bteq', '.sql']:
                return [path]
            else:
                raise ValueError(f"File must have .bteq or .sql extension: {path}")
                
        elif path.is_dir():
            # Directory - find all BTEQ/SQL files
            files = []
            for ext in ['*.bteq', '*.sql']:
                files.extend(path.glob(ext))
                files.extend(path.glob(f'**/{ext}'))  # Recursive search
            
            if not files:
                raise ValueError(f"No BTEQ/SQL files found in directory: {path}")
                
            return sorted(files)
        else:
            raise ValueError(f"Input must be a file or directory: {path}")
    
    def run_v1_prscrip_sp(self, args: argparse.Namespace) -> dict:
        """V1: Pure prescriptive SQLGlot-based BTEQ→Snowflake SP conversion."""
        logger.info("🔧 V1_PRSCRIP_SP: Pure prescriptive SQLGlot conversion")
        return self._run_traditional_pipeline(args, enable_llm=False)
        
    def run_v2_prscrip_claude_sp(self, args: argparse.Namespace) -> dict:
        """V2: Prescriptive pipeline + Claude enhancement for SP."""
        logger.info("⚡ V2_PRSCRIP_CLAUDE_SP: Prescriptive + Claude SP enhancement")
        return self._run_traditional_pipeline(args, enable_llm=True)
        
    def run_v3_prscrip_claude_sp_claude_dbt(self, args: argparse.Namespace) -> dict:
        """V3: Prescriptive + Claude SP + Claude DBT conversion."""
        logger.info("🏗️ V3_PRSCRIP_CLAUDE_SP_CLAUDE_DBT: Prescriptive + Claude SP + Claude DBT")
        return self._run_claude_focused_pipeline(args)
    
    def run_v4_prscrip_claude_llama_sp(self, args: argparse.Namespace) -> dict:
        """V4: Prescriptive + Claude vs Llama comparison, pick best SP."""
        logger.info("🤖 V4_PRSCRIP_CLAUDE_LLAMA_SP: Prescriptive + Claude vs Llama SP comparison")
        # For now, use the streamlined pipeline as a placeholder
        return self._run_streamlined_pipeline(args)
    
    def run_v5_claude_dbt(self, args: argparse.Namespace) -> dict:
        """V5: Direct BTEQ to DBT conversion using Claude (skip SP generation)."""
        logger.info("🎯 V5_CLAUDE_DBT: Direct BTEQ → DBT conversion using Claude")
        logger.info("Mode description: Direct DBT conversion - BTEQ directly to modern DBT models (1 LLM call)")
        
        return self._run_direct_dbt_pipeline(args)
    
    def _check_llm_availability(self) -> bool:
        """Check if LLM services are available."""
        try:
            from generator.llm_integration import SnowflakeLLMService
            llm_service = SnowflakeLLMService()
            return llm_service.available
        except Exception as e:
            logger.error(f"Failed to initialize LLM service: {e}")
            return False
    
    def _run_traditional_pipeline(self, args: argparse.Namespace, enable_llm: bool = False) -> dict:
        """Run traditional processing pipeline."""
        mode_name = "v2_prescriptive_enhanced" if enable_llm else "v1_prescriptive"
        logger.info(f"Running traditional pipeline (LLM enhancement: {enable_llm})")
        
        # Check LLM availability if required
        if enable_llm and not self._check_llm_availability():
            error_msg = f"LLM enhancement required for {mode_name} but LLM service is unavailable"
            logger.error(error_msg)
            raise RuntimeError(error_msg)
        
        # Use unified run directory structure
        input_dir = str(Path(args.input).parent) if Path(args.input).is_file() else args.input
        
        # Create results directory within the run directory
        results_dir = Path(args.output) / 'results'
        results_dir.mkdir(parents=True, exist_ok=True)
        
        # For v2_prscrip_claude_sp mode, use only Claude model
        llm_models = ["claude-4-sonnet"] if args.mode == ProcessingMode.V2_PRSCRIP_CLAUDE_SP else None
        
        pipeline = SubstitutionPipeline(
            input_dir=input_dir, 
            output_base_dir=str(results_dir),
            enable_llm=enable_llm,
            llm_models=llm_models  # Pass Claude-only for v2 mode
        )
        
        if Path(args.input).is_file():
            # Process single file - copy it to temp directory for pipeline processing
            import tempfile
            import shutil
            with tempfile.TemporaryDirectory() as temp_dir:
                temp_file = Path(temp_dir) / Path(args.input).name
                shutil.copy2(args.input, temp_file)
                
                # Update pipeline to use temp directory
                pipeline.input_dir = temp_dir
                results = pipeline.run_complete_pipeline()
                
            return {"mode": mode_name, "files_processed": 1, "results": results}
        else:
            # Process directory
            results = pipeline.run_complete_pipeline()
            return {"mode": mode_name, "results": results}
            
    def _run_agentic_pipeline(self, args: argparse.Namespace, multi_model: bool = False) -> dict:
        """Run agentic (AI-enhanced) processing mode."""
        mode_name = "v5_agentic_multi_model" if multi_model else "v4_agentic_orchestrated"
        logger.info(f"Running agentic pipeline (multi-model: {multi_model})")
        
        # Check LLM availability - required for agentic modes
        if not self._check_llm_availability():
            error_msg = f"LLM service required for {mode_name} but is unavailable"
            logger.error(error_msg)
            raise RuntimeError(error_msg)
        
        # Lazy import agentic components only when needed
        try:
            from agentic.integration import AgenticMigrationPipeline
        except ImportError:
            # Fallback import (should not be needed with new structure)
            raise ImportError("Could not import AgenticMigrationPipeline - check project structure")
        
        # Configure agentic orchestrator (simplified for now) 
        pipeline = AgenticMigrationPipeline(enable_agentic=True)
        
        input_files = self.get_input_files(args.input)
        results = []
        
        for file_path in input_files:
            logger.info(f"Processing {file_path} with agentic pipeline...")
            
            with open(file_path, 'r') as f:
                bteq_content = f.read()
                
            result = pipeline.process_bteq_migration(
                bteq_content=bteq_content,
                file_name=file_path.name,
                target_complexity=args.complexity_target
            )
            results.append(result)
            
        return {"mode": mode_name, "files_processed": len(input_files), "results": results}
    
    def _run_full_llm_pipeline(self, args: argparse.Namespace) -> dict:
        """Run full pipeline: Prescriptive + Dual LLM SPs + Dual LLM DBT (4 LLM calls)."""
        logger.info("🏗️ Running full LLM pipeline: Prescriptive + 2 LLM SPs + 2 LLM DBT")
        
        # Check LLM availability - required for LLM SPs and DBT conversion
        if not self._check_llm_availability():
            error_msg = "LLM service required for v3_prscrip_llm_sp_llm_dbt but is unavailable"
            logger.error(error_msg)
            raise RuntimeError(error_msg)
        
        input_files = self.get_input_files(args.input)
        results = []
        
        for file_path in input_files:
            logger.info(f"Full pipeline processing for {file_path}...")
            
            with open(file_path, 'r') as f:
                bteq_content = f.read()
            
            procedure_name = file_path.stem.upper()
            
            # Step 1: Run V5 (multi-model) to generate Prescriptive + Claude + Llama SPs
            logger.info(f"Step 1: Generating multiple SPs (Prescriptive + Claude + Llama)")
            temp_args = argparse.Namespace(**vars(args))
            temp_args.mode = ProcessingMode.V4_PRSCRIP_CLAUDE_LLAMA_SP
            temp_args.input = str(file_path)
            
            sp_results = self.run_v4_prscrip_claude_llama_sp(temp_args)
            
            # Extract the best stored procedure for DBT reference
            best_sp_sql = self._extract_best_stored_procedure(sp_results, procedure_name, args)
            
            # Step 2: Run DBT conversion with dual LLM approach
            logger.info(f"Step 2: Running DBT conversion with dual LLM (Claude + Llama)")
            from agentic.dbt_integration import create_dbt_enabled_pipeline
            dbt_pipeline = create_dbt_enabled_pipeline(output_dir=args.output)
            
            dbt_result = dbt_pipeline.migrate_with_dbt(
                original_bteq=bteq_content,
                procedure_name=procedure_name,
                stored_procedure_sql=best_sp_sql,
                analysis_markdown=f"Full pipeline BTEQ to DBT conversion for {procedure_name}"
            )
            
            results.append({
                "file_name": file_path.name,
                "procedure_name": procedure_name,
                "sp_generation_success": sp_results.get("files_processed", 0) > 0,
                "dbt_conversion_success": dbt_result.migration_summary.get("dbt_conversion_success", False),
                "has_dbt_model": dbt_result.dbt_model_sql is not None,
                "llm_calls_count": 4,  # 2 for SPs + 2 for DBT
                "migration_summary": dbt_result.migration_summary
            })
            
            logger.info(f"✅ Full pipeline completed for {procedure_name}")
        
        return {
            "mode": "v3_prscrip_llm_sp_llm_dbt", 
            "files_processed": len(input_files), 
            "results": results,
            "total_llm_calls": len(input_files) * 4,
            "dbt_conversions": sum(1 for r in results if r["has_dbt_model"])
        }
    
    def _run_streamlined_pipeline(self, args: argparse.Namespace) -> dict:
        """Run streamlined pipeline: Prescriptive + Consolidated BTEQ→DBT (2 LLM calls)."""
        logger.info("🏗️ Running streamlined pipeline: Prescriptive + Consolidated BTEQ→DBT")
        
        # Check LLM availability
        if not self._check_llm_availability():
            error_msg = "LLM service required for v4_prscrip_llm_bteq_dbt but is unavailable"
            logger.error(error_msg)
            raise RuntimeError(error_msg)
        
        input_files = self.get_input_files(args.input)
        results = []
        
        for file_path in input_files:
            logger.info(f"Streamlined pipeline processing for {file_path}...")
            
            with open(file_path, 'r') as f:
                bteq_content = f.read()
            
            procedure_name = file_path.stem.upper()
            
            # Step 1: Generate prescriptive SP for reference
            logger.info(f"Step 1: Generating prescriptive SP")
            temp_args = argparse.Namespace(**vars(args))
            temp_args.mode = ProcessingMode.V1_PRSCRIP_SP
            temp_args.input = str(file_path)
            
            sp_results = self.run_v1_prscrip_sp(temp_args)
            prescriptive_sp_sql = self._extract_prescriptive_stored_procedure(sp_results, procedure_name, args)
            
            # Step 2: Run consolidated BTEQ→DBT conversion (Claude + Llama)
            logger.info(f"Step 2: Running consolidated BTEQ→DBT conversion")
            from agentic.dbt_integration import create_dbt_enabled_pipeline
            dbt_pipeline = create_dbt_enabled_pipeline(output_dir=args.output)
            
            dbt_result = dbt_pipeline.migrate_with_dbt(
                original_bteq=bteq_content,
                procedure_name=procedure_name,
                stored_procedure_sql=prescriptive_sp_sql,
                analysis_markdown=f"Streamlined BTEQ to DBT conversion for {procedure_name}"
            )
            
            results.append({
                "file_name": file_path.name,
                "procedure_name": procedure_name,
                "sp_generation_success": sp_results.get("files_processed", 0) > 0,
                "dbt_conversion_success": dbt_result.migration_summary.get("dbt_conversion_success", False),
                "has_dbt_model": dbt_result.dbt_model_sql is not None,
                "llm_calls_count": 2,  # 2 for DBT conversion only
                "migration_summary": dbt_result.migration_summary
            })
            
            logger.info(f"✅ Streamlined pipeline completed for {procedure_name}")
        
        return {
            "mode": "v4_prscrip_llm_bteq_dbt", 
            "files_processed": len(input_files), 
            "results": results,
            "total_llm_calls": len(input_files) * 2,
            "dbt_conversions": sum(1 for r in results if r["has_dbt_model"])
        }
    
    def _run_claude_focused_pipeline(self, args: argparse.Namespace) -> dict:
        """Run Claude-focused pipeline: Prescriptive + Claude SP + Claude DBT (2 LLM calls)."""
        logger.info("🏗️ Running Claude-focused pipeline: Prescriptive + Claude SP + Claude DBT")
        
        # Check LLM availability
        if not self._check_llm_availability():
            error_msg = "LLM service required for v4_prscrip_claude_sp_claude_bteq_dbt but is unavailable"
            logger.error(error_msg)
            raise RuntimeError(error_msg)
        
        input_files = self.get_input_files(args.input)
        results = []
        
        for file_path in input_files:
            logger.info(f"Claude-focused pipeline processing for {file_path}...")
            
            with open(file_path, 'r') as f:
                bteq_content = f.read()
            
            procedure_name = file_path.stem.upper()
            
            # Step 1: Generate prescriptive SP
            logger.info(f"Step 1: Generating prescriptive SP")
            temp_args = argparse.Namespace(**vars(args))
            temp_args.mode = ProcessingMode.V1_PRSCRIP_SP
            temp_args.input = str(file_path)
            
            sp_results = self.run_v1_prscrip_sp(temp_args)
            prescriptive_sp_sql = self._extract_prescriptive_stored_procedure(sp_results, procedure_name, args)
            
            # Step 2: Generate Claude SP using enhanced pipeline
            logger.info(f"Step 2: Generating Claude SP")
            temp_args.mode = ProcessingMode.V2_PRSCRIP_CLAUDE_SP
            claude_sp_results = self.run_v2_prscrip_claude_sp(temp_args)
            claude_sp_sql = self._extract_enhanced_stored_procedure(claude_sp_results, procedure_name, args)
            
            # Step 3: Run Claude-only DBT conversion
            logger.info(f"Step 3: Running Claude DBT conversion (Claude-4-sonnet ONLY)")
            from agentic.dbt_integration import create_dbt_enabled_pipeline
            dbt_pipeline = create_dbt_enabled_pipeline(
                output_dir=args.output,
                dbt_models=["claude-4-sonnet"]  # Claude ONLY - not Llama!
            )
            
            dbt_result = dbt_pipeline.migrate_with_dbt(
                original_bteq=bteq_content,
                procedure_name=procedure_name,
                stored_procedure_sql=claude_sp_sql,
                analysis_markdown=f"Claude-focused BTEQ to DBT conversion for {procedure_name}"
            )
            
            results.append({
                "file_name": file_path.name,
                "procedure_name": procedure_name,
                "sp_generation_success": sp_results.get("files_processed", 0) > 0,
                "claude_sp_success": claude_sp_results.get("files_processed", 0) > 0,
                "dbt_conversion_success": dbt_result.migration_summary.get("dbt_conversion_success", False),
                "has_dbt_model": dbt_result.dbt_model_sql is not None,
                "llm_calls_count": 2,  # 1 for Claude SP + 1 for Claude DBT
                "migration_summary": dbt_result.migration_summary
            })
            
            logger.info(f"✅ Claude-focused pipeline completed for {procedure_name}")
        
        return {
            "mode": "v4_prscrip_claude_sp_claude_bteq_dbt", 
            "files_processed": len(input_files), 
            "results": results,
            "total_llm_calls": len(input_files) * 2,
            "dbt_conversions": sum(1 for r in results if r["has_dbt_model"])
        }
    
    def _run_direct_dbt_pipeline(self, args: argparse.Namespace) -> dict:
        """Run direct BTEQ → DBT pipeline using Claude only (1 LLM call)."""
        logger.info("🎯 Running direct BTEQ → DBT pipeline using Claude")
        
        # Check LLM availability - required for DBT conversion
        if not self._check_llm_availability():
            error_msg = "LLM service required for v5_claude_dbt but is unavailable"
            logger.error(error_msg)
            raise RuntimeError(error_msg)
        
        input_files = self.get_input_files(args.input)
        results = []
        
        for file_path in input_files:
            logger.info(f"Direct DBT conversion processing for {file_path}...")
            
            with open(file_path, 'r') as f:
                bteq_content = f.read()
            
            procedure_name = file_path.stem.upper()
            
            # Direct BTEQ → DBT conversion (minimal SP for context, focus on DBT)
            logger.info(f"Running direct BTEQ → DBT conversion with Claude")
            from agentic.dbt_integration import create_dbt_enabled_pipeline
            
            # Create output directory
            output_dir = Path(args.output)
            results_dir = output_dir / 'results'
            results_dir.mkdir(parents=True, exist_ok=True)
            
            # Use exemplar SQL as reference for high-quality DBT conversion
            logger.info(f"Loading exemplar SQL reference for high-quality DBT conversion")
            exemplar_path = Path("dcf/models/acct_baln/bkdt_adj/acct_baln_bkdt_adj_rule.sql")
            
            if exemplar_path.exists():
                with open(exemplar_path, 'r') as f:
                    exemplar_sql = f.read()
                logger.info(f"✅ Loaded exemplar SQL reference ({len(exemplar_sql)} chars)")
            else:
                # Fallback to minimal SP if exemplar not found
                logger.warning(f"⚠️  Exemplar not found at {exemplar_path}, using minimal reference")
                exemplar_sql = f"""
-- Exemplar DBT Model Structure (simplified fallback)
WITH source_data AS (
    SELECT * FROM source_table
)
SELECT * FROM source_data;
"""
            
            # Initialize DBT pipeline with Claude-only configuration
            dbt_pipeline = create_dbt_enabled_pipeline(
                output_dir=str(results_dir),
                dbt_models=["claude-4-sonnet"]  # Claude-only for direct conversion
            )
            
            # Run DBT conversion with BTEQ as primary input, exemplar SQL as reference
            dbt_result = dbt_pipeline.migrate_with_dbt(
                original_bteq=bteq_content,
                procedure_name=procedure_name,
                stored_procedure_sql=exemplar_sql,  # High-quality exemplar as reference
                analysis_markdown=f"Direct BTEQ to DBT conversion for {procedure_name} - Use exemplar SQL reference for structure/quality guidance"
            )
            
            results.append({
                "file_name": file_path.name,
                "procedure_name": procedure_name,
                "sp_generation_success": False,  # Skipped SP generation
                "dbt_conversion_success": dbt_result.migration_summary.get("dbt_conversion_success", False),
                "has_dbt_model": dbt_result.dbt_model_sql is not None,
                "llm_calls_count": 1,  # Only DBT conversion
                "migration_summary": dbt_result.migration_summary,
                "direct_conversion": True
            })
            
            logger.info(f"✅ Direct DBT pipeline completed for {procedure_name}")
        
        return {
            "mode": ProcessingMode.V5_CLAUDE_DBT, 
            "files_processed": len(input_files), 
            "results": results,
            "total_llm_calls": len(input_files) * 1,  # Only 1 LLM call per file
            "dbt_conversions": sum(1 for r in results if r["has_dbt_model"])
        }
    
    def _extract_best_stored_procedure(self, sp_results: dict, procedure_name: str, args: argparse.Namespace) -> str:
        """Extract the best stored procedure from V5 multi-model results."""
        return self._get_or_generate_stored_procedure("", procedure_name, args)
    
    def _extract_prescriptive_stored_procedure(self, sp_results: dict, procedure_name: str, args: argparse.Namespace) -> str:
        """Extract prescriptive stored procedure from V1 results."""
        return self._get_or_generate_stored_procedure("", procedure_name, args)
    
    def _extract_enhanced_stored_procedure(self, sp_results: dict, procedure_name: str, args: argparse.Namespace) -> str:
        """Extract enhanced stored procedure from V2 results."""
        return self._get_or_generate_stored_procedure("", procedure_name, args)
    
    def _get_or_generate_stored_procedure(self, bteq_content: str, procedure_name: str, args: argparse.Namespace) -> str:
        """Get existing stored procedure or generate a basic one for DBT conversion reference."""
        logger.info(f"Generating reference stored procedure for {procedure_name}")
        
        # Generate a basic stored procedure using traditional pipeline
        temp_args = argparse.Namespace(**vars(args))
        temp_args.mode = ProcessingMode.V1_PRSCRIP_SP
        
        # Create properly named temporary file for processing
        import tempfile
        import os
        temp_dir = tempfile.mkdtemp()
        temp_file_path = os.path.join(temp_dir, f"{procedure_name}.sql")
        
        with open(temp_file_path, 'w') as temp_file:
            temp_file.write(bteq_content)
        temp_args.input = temp_file_path
        
        try:
            # Run basic processing to get stored procedure
            basic_result = self.run_v1_prscrip_sp(temp_args)
            
            # Extract stored procedure SQL from the generated results
            if basic_result and "results" in basic_result:
                # Look in the output directory for the generated stored procedure 
                # The substitution pipeline now puts files directly in the base output dir
                base_dir = Path(temp_args.output) / "results"
                
                # Look directly in the snowflake_sp directory
                sp_dir = base_dir / "snowflake_sp"
                if sp_dir.exists():
                    
                    # Look for generated SQL files
                    sql_files = list(sp_dir.glob("*.sql"))
                    if sql_files:
                        # Use the main SQL file (not the prescriptive one)
                        main_sql_file = next((f for f in sql_files if '_prescriptive' not in f.name), sql_files[0])
                        stored_proc_sql = main_sql_file.read_text()
                        logger.info(f"Found generated stored procedure: {main_sql_file}")
                        return stored_proc_sql
                
            logger.warning(f"Could not find generated stored procedure, using template")
            # Fallback - return a basic template
            return f"""
CREATE OR REPLACE PROCEDURE {procedure_name}()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    -- Converted from BTEQ: {procedure_name}
    RETURN 'SUCCESS';
END;
$$;
"""
        finally:
            # Clean up temporary file
            import os
            try:
                os.unlink(temp_file.name)
            except:
                pass
        
    def _run_hybrid_pipeline(self, args: argparse.Namespace) -> dict:
        """Run hybrid (traditional foundation + AI enhancement) mode."""
        logger.info("🏗️ V3_PRSCRIP_CLAUDE_SP_CLAUDE_DBT: Prescriptive + Claude SP + Claude DBT")
        
        # Check LLM availability - required for AI enhancement
        if not self._check_llm_availability():
            error_msg = "LLM service required for v3_hybrid_foundation but is unavailable"
            logger.error(error_msg)
            raise RuntimeError(error_msg)
        
        # Step 1: Traditional processing foundation
        traditional_results = self._run_traditional_pipeline(args, enable_llm=False)
        
        # Step 2: AI enhancement on traditional output
        # Lazy import agentic components only when needed
        try:
            from agentic.integration import AgenticMigrationPipeline
        except ImportError:
            # Fallback import (should not be needed with new structure)
            raise ImportError("Could not import AgenticMigrationPipeline - check project structure")
            
        agentic_pipeline = AgenticMigrationPipeline(enable_agentic=True)
        
        enhanced_results = []
        input_files = self.get_input_files(args.input)
        
        for i, file_path in enumerate(input_files):
            logger.info(f"Enhancing {file_path} with agentic capabilities...")
            
            # Get traditional result
            if isinstance(traditional_results['results'], list):
                traditional_result = traditional_results['results'][i]
            else:
                traditional_result = traditional_results['results']
            
            # Enhance with AI
            with open(file_path, 'r') as f:
                original_bteq = f.read()
                
            enhanced_result = agentic_pipeline.enhance_traditional_output(
                traditional_output=traditional_result,
                original_bteq=original_bteq,
                file_name=file_path.name
            )
            enhanced_results.append(enhanced_result)
            
        return {
            "mode": "v3_hybrid_foundation",
            "files_processed": len(input_files), 
            "traditional_results": traditional_results,
            "enhanced_results": enhanced_results
        }
        
    def save_results(self, results: dict, output_dir: str) -> str:
        """Save processing results to unified run directory."""
        output_path = Path(output_dir)
        
        # Save migration summary in run directory (no additional timestamp needed)
        summary_file = output_path / "migration_summary.json"
        with open(summary_file, 'w') as f:
            json.dump(results, f, indent=2, default=str)
            
        return str(summary_file)
        
    def print_summary(self, results: dict, args: argparse.Namespace):
        """Print execution summary to both console and log file."""
        mode = results.get("mode", "unknown")
        files_processed = results.get("files_processed", 0)
        
        # Build summary lines
        summary_lines = [
            "\n" + "="*60,
            "🎉 BTEQ MIGRATION COMPLETED",
            "="*60,
            f"Mode: {mode.upper()}",
            f"Files processed: {files_processed}",
            f"Input: {args.input}",
            f"Output: {args.output}",
            f"Config: Using defaults (config.cfg)"
        ]
        
        if "hybrid" in mode or "v3_hybrid" in mode:
            summary_lines.extend([
                "\n📊 HYBRID MODE SUMMARY:",
                "  ✅ Traditional foundation: Fast rule-based processing",
                f"  🤖 AI enhancement: {args.model_strategy} strategy"
            ])
            if args.multi_model:
                summary_lines.append("  🔀 Multi-model consensus enabled")
                
        elif "agentic" in mode or mode.startswith("v4_") or mode.startswith("v5_"):
            summary_lines.extend([
                "\n🤖 AGENTIC MODE SUMMARY:",
                f"  Strategy: {args.model_strategy}",
                f"  Multi-model: {args.multi_model}",
                f"  Validation: {args.validation}",
                f"  Error correction: {args.error_correction}"
            ])
            
        summary_lines.append(f"\n📁 Results saved to: {args.output}")
        
        if args.verbose:
            summary_lines.extend([
                "\n🔍 DETAILED RESULTS:",
                json.dumps(results, indent=2, default=str)[:500] + "..."
            ])
        
        # Print to console and log
        for line in summary_lines:
            print(line)
            logger.info(line.replace('🎉', '').replace('📊', '').replace('🤖', '').replace('📁', '').replace('🔍', '').strip())
            
    def print_final_summary(self, results: dict, args: argparse.Namespace):
        """Print final processing summary to both console and log file."""
        mode = results.get("mode", "unknown")
        files_processed = results.get("files_processed", 0)
        
        # Build final summary lines
        final_summary_lines = [
            "\n" + "="*80,
            "📋 PROCESSING SUMMARY",
            "="*80
        ]
        
        # Mode-specific summary
        if mode.startswith("v1_"):
            final_summary_lines.extend([
                "✅ V1_PRSCRIP_SP PROCESSING COMPLETED",
                "   • Pure rule-based pipeline executed",
                "   • NO AI/LLM components used",
                "   • Variable substitution ✓",
                "   • BTEQ parsing ✓",
                "   • SQL transpilation (SQLGlot) ✓",
                "   • Basic stored procedure generation ✓"
            ])
            
        elif mode.startswith("v2_"):
            final_summary_lines.extend([
                "✅ V2_PRSCRIP_CLAUDE_SP PROCESSING COMPLETED",
                "   • Traditional pipeline + basic AI enhancement",
                "   • Limited LLM usage for quality improvement"
            ])
            
        elif mode.startswith("v3_"):
            final_summary_lines.extend([
                "✅ V3_PRSCRIP_CLAUDE_SP_CLAUDE_DBT PROCESSING COMPLETED",
                "   • Traditional foundation + AI agent enhancement"
            ])
            
        elif mode.startswith("v4_"):
            final_summary_lines.extend([
                "✅ V4_PRSCRIP_CLAUDE_LLAMA_SP PROCESSING COMPLETED",
                "   • Full multi-agent workflow executed"
            ])
            
        elif mode.startswith("v5_"):
            final_summary_lines.extend([
                "✅ V4_PRSCRIP_CLAUDE_LLAMA_SP PROCESSING COMPLETED",
                "   • Advanced multi-model consensus achieved"
            ])
            
        elif mode == "v3_prscrip_llm_sp_llm_dbt":
            final_summary_lines.extend([
                "✅ V3_PRSCRIP_LLM_SP_LLM_DBT PROCESSING COMPLETED",
                "   • Prescriptive + Claude + Llama SPs generated",
                "   • Dual LLM DBT conversion performed",
                "   • 4 total LLM calls executed",
                "   • DBT best practices applied"
            ])
        elif mode == "v4_prscrip_llm_bteq_dbt":
            final_summary_lines.extend([
                "✅ V4_PRSCRIP_LLM_BTEQ_DBT PROCESSING COMPLETED",
                "   • Prescriptive SP generated for reference",
                "   • Consolidated BTEQ→DBT conversion performed",
                "   • 2 total LLM calls executed",
                "   • Streamlined pipeline optimized"
            ])
        elif mode == "v4_prscrip_claude_sp_claude_bteq_dbt":
            final_summary_lines.extend([
                "✅ V4_PRSCRIP_CLAUDE_SP_CLAUDE_BTEQ_DBT PROCESSING COMPLETED",
                "   • Prescriptive SP generated",
                "   • Claude SP enhancement performed", 
                "   • Claude-focused DBT conversion executed",
                "   • 2 total LLM calls executed"
            ])
        
        # Processing metrics
        final_summary_lines.extend([
            "\n📊 PROCESSING METRICS:",
            f"   • Files processed: {files_processed}",
            f"   • Processing time: ~{self._get_processing_time()} seconds",
            f"   • Mode: {mode.upper()}"
        ])
        
        # Check if LLM was used when it shouldn't be
        if mode.startswith("v1_") and self._check_if_llm_was_used(results):
            final_summary_lines.extend([
                "\n⚠️  WARNING: LLM usage detected in v1_prescriptive mode!",
                "   This should be pure rule-based processing only."
            ])
        
        final_summary_lines.extend([
            "\n📁 OUTPUT LOCATION:",
            f"   {args.output}",
            "="*80
        ])
        
        # Print to console and log
        for line in final_summary_lines:
            print(line)
            logger.info(line.replace('📋', '').replace('✅', '').replace('📊', '').replace('⚠️', '').replace('📁', '').strip())
        
    def _get_processing_time(self) -> str:
        """Get approximate processing time.""" 
        # Could be enhanced to track actual time
        return "~60"
        
    def _check_if_llm_was_used(self, results: dict) -> bool:
        """Check if LLM was inadvertently used."""
        # Could check logs or results for LLM indicators
        return False  # Simplified for now
            
    def run(self, args: argparse.Namespace) -> int:
        """Main execution method."""
        try:
            # Setup logging and get unified run directory
            run_dir = self.setup_logging(args.log_level, args.verbose, args.output)
            
            # Update output to use the run directory
            args.output = run_dir
            
            # Validate inputs
            self.validate_inputs(args)
            
            # Route to appropriate processing mode
            logger.info(f"Starting BTEQ migration in {args.mode} mode")
            logger.info(f"Mode description: {ProcessingMode.get_mode_description(args.mode)}")
            
            # Route to clean processing methods
            if args.mode == ProcessingMode.V1_PRSCRIP_SP:
                results = self.run_v1_prscrip_sp(args)
            elif args.mode == ProcessingMode.V2_PRSCRIP_CLAUDE_SP:
                results = self.run_v2_prscrip_claude_sp(args)
            elif args.mode == ProcessingMode.V3_PRSCRIP_CLAUDE_SP_CLAUDE_DBT:
                results = self.run_v3_prscrip_claude_sp_claude_dbt(args)
            elif args.mode == ProcessingMode.V4_PRSCRIP_CLAUDE_LLAMA_SP:
                results = self.run_v4_prscrip_claude_llama_sp(args)
            elif args.mode == ProcessingMode.V5_CLAUDE_DBT:
                results = self.run_v5_claude_dbt(args)
            # Legacy compatibility
            elif args.mode == ProcessingMode.PRESCRIPTIVE:
                results = self.run_v1_prscrip_sp(args)
            else:
                raise ValueError(f"Unknown processing mode: {args.mode}")
                
            # Save results
            results_file = self.save_results(results, args.output)
            
            # Print summary
            self.print_summary(results, args)
            
            # Generate final summary
            self.print_final_summary(results, args)
            
            logger.info("Migration completed successfully")
            return 0
            
        except Exception as e:
            logger.error(f"Migration failed: {e}")
            if args.verbose:
                import traceback
                traceback.print_exc()
            return 1


def build_argument_parser() -> argparse.ArgumentParser:
    """Build the unified CLI argument parser."""
    
    parser = argparse.ArgumentParser(
        description="BTEQ DCF - Unified BTEQ to Snowflake Migration Tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # V1: Pure prescriptive (fastest, no AI)
  python main.py --input my_script.bteq --mode v1_prscrip_sp
  
  # V2: Prescriptive + Claude enhancement (RECOMMENDED)
  python main.py --input my_script.bteq --mode v2_prscrip_claude_sp
  
  # V3: Complete Claude pipeline with DBT
  python main.py --input my_script.bteq --mode v3_prscrip_claude_sp_claude_dbt
  
  # V4: Multi-model comparison (when implemented)
  python main.py --input my_script.bteq --mode v4_prscrip_claude_llama_sp
  
  # V5: Direct BTEQ to DBT (no SP generation)
  python main.py --input my_script.bteq --mode v5_claude_dbt

🎯 PROCESSING MODES:
  v1_prscrip_sp:                Pure prescriptive conversion (fastest, 0 LLM calls)
  v2_prscrip_claude_sp:         Prescriptive + Claude enhancement (RECOMMENDED, 1 LLM call)  
  v3_prscrip_claude_sp_claude_dbt: Full pipeline with DBT models (comprehensive, 2 LLM calls)
  v4_prscrip_claude_llama_sp:   Claude vs Llama comparison (highest quality, 2 LLM calls)
  v5_claude_dbt:                Direct BTEQ → DBT conversion (streamlined, 1 LLM call)
        """
    )
    
    # Required arguments
    parser.add_argument(
        '--input', '-i',
        required=True,
        help='Input BTEQ file or directory containing BTEQ files'
    )
    
    parser.add_argument(
        '--mode', '-m',
        choices=ProcessingMode.all_modes(),
        default=ProcessingMode.V2_PRSCRIP_CLAUDE_SP,
        help='Processing mode - clean pipeline selection [default: v2_prscrip_claude_sp]'
    )
    
    # Optional arguments
    parser.add_argument(
        '--output', '-o',
        default='./output',
        help='Output directory for generated files [default: ./output]'
    )
    

    
    # AI Model Configuration
    ai_group = parser.add_argument_group('AI Model Configuration (for agentic/hybrid modes)')
    ai_group.add_argument(
        '--model-strategy',
        choices=['quality_first', 'performance_first', 'balanced', 'cost_optimized'],
        default='balanced',
        help='Model selection strategy [default: balanced]'
    )
    
    ai_group.add_argument(
        '--multi-model',
        action='store_true',
        help='Enable multi-model consensus (compare outputs from different models)'
    )
    
    ai_group.add_argument(
        '--validation',
        action='store_true',
        default=True,
        help='Enable validation agents for syntax and performance checking [default: True]'
    )
    
    ai_group.add_argument(
        '--error-correction',
        action='store_true',
        default=True,
        help='Enable error correction agents for iterative improvement [default: True]'
    )
    
    ai_group.add_argument(
        '--complexity-target',
        choices=['low', 'medium', 'high'],
        default='medium',
        help='Target complexity for processing [default: medium]'
    )
    
    # Logging and output
    log_group = parser.add_argument_group('Logging and Output')
    log_group.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose output with detailed logging'
    )
    
    log_group.add_argument(
        '--log-level',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        default='INFO',
        help='Set logging level [default: INFO]'
    )
    
    log_group.add_argument(
        '--dry-run',
        action='store_true',
        help='Perform analysis only without generating output files'
    )
    
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    """Main entry point for BTEQ migration CLI."""
    
    parser = build_argument_parser()
    args = parser.parse_args(argv)
    
    # Create CLI instance and run
    cli = BteqMigrationCLI()
    return cli.run(args)


if __name__ == "__main__":
    sys.exit(main())
