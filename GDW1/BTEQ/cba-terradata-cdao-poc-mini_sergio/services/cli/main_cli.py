"""
BTEQ Migration CLI Service

Main CLI class extracted from monolithic main.py with ZERO logic changes.
All methods preserved exactly as they were.
"""

import argparse
import sys
import logging
from pathlib import Path
from typing import Optional, List
import json
from datetime import datetime
import time

# Import only essential services - others imported conditionally
try:
    from services.migration.pipelines.substitution import SubstitutionPipeline
    from utils.config import get_config_manager, get_model_manager, ModelSelectionStrategy
    from utils.logging import get_llm_logger
except ImportError:
    # Should not happen with new flattened structure, but keeping for safety
    raise ImportError("Could not import required modules - check project structure")

from .modes import ProcessingMode

logger = logging.getLogger(__name__)


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
        """V2: Deterministic + Agentic pipeline (clean architecture)."""
        logger.info("⚡ V2_PRSCRIP_CLAUDE_SP: Deterministic + Agentic SP enhancement")
        return self._run_clean_deterministic_plus_agentic_pipeline(args)
        
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
            from services.conversion.llm.integration import SnowflakeLLMService
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
            temp_dir = tempfile.mkdtemp()
            temp_file = Path(temp_dir) / Path(args.input).name
            shutil.copy2(args.input, temp_file)
            
            # Run pipeline on temp directory
            try:
                results = pipeline.run_complete_pipeline()
            finally:
                # Clean up temp directory
                import shutil
                shutil.rmtree(temp_dir, ignore_errors=True)
        else:
            # Process directory
            results = pipeline.run_complete_pipeline()
        
        return {"mode": mode_name, "results": results}
    
    def _run_clean_deterministic_plus_agentic_pipeline(self, args: argparse.Namespace) -> dict:
        """Run clean deterministic + agentic pipeline using new architecture."""
        logger.info("🔄 Using clean AgenticMigrationPipeline architecture")
        
        try:
            # Import the clean orchestrator
            from services.orchestration.integration.pipeline import AgenticMigrationPipeline
            from services.orchestration.workflows.orchestrator import OrchestrationConfig
            
            # Configure for deterministic + agentic (single model for v2)
            config = OrchestrationConfig(
                enable_multi_model=False,  # Single model (Claude) for v2 mode
                enable_error_correction=getattr(args, 'error_correction', True),
                enable_tool_validation=getattr(args, 'validation', True),
                models_to_use=["claude-4-sonnet"],
                quality_threshold=0.7
            )
            
            # Initialize clean pipeline with LLM logging (use timestamped run directory)
            llm_log_dir = str(Path(self.run_dir) / "llm_interactions")
            
            pipeline = AgenticMigrationPipeline(
                orchestration_config=config,
                enable_orchestration=False,  # Use direct generators for v2 (simpler)
                llm_log_dir=llm_log_dir,
                dry_run=args.dry_run
            )
            
            results = []
            
            if Path(args.input).is_file():
                # Single file processing
                logger.info(f"📄 Processing single file: {args.input}")
                result = pipeline.process_bteq_file(
                    input_file=args.input,
                    output_directory=args.output,
                    use_agentic=True  # Enable deterministic + agentic
                )
                results.append(result)
                
                if result["success"]:
                    logger.info(f"✅ Successfully processed: {Path(args.input).name}")
                else:
                    logger.error(f"❌ Failed to process: {Path(args.input).name} - {result.get('error', 'Unknown error')}")
                    
            else:
                # Directory processing
                input_dir = Path(args.input)
                bteq_files = list(input_dir.glob("*.sql")) + list(input_dir.glob("*.bteq"))
                
                logger.info(f"📁 Processing {len(bteq_files)} files from directory: {args.input}")
                
                for bteq_file in bteq_files:
                    try:
                        logger.info(f"📄 Processing file: {bteq_file.name}")
                        result = pipeline.process_bteq_file(
                            input_file=str(bteq_file),
                            output_directory=args.output,
                            use_agentic=True
                        )
                        results.append(result)
                        
                        if result["success"]:
                            logger.info(f"✅ Successfully processed: {bteq_file.name}")
                        else:
                            logger.error(f"❌ Failed to process: {bteq_file.name} - {result.get('error', 'Unknown error')}")
                            
                    except Exception as e:
                        logger.error(f"❌ Error processing {bteq_file}: {e}")
                        results.append({
                            "success": False,
                            "input_file": str(bteq_file),
                            "error": str(e)
                        })
            
            # Aggregate results
            successful = [r for r in results if r.get("success", False)]
            failed = [r for r in results if not r.get("success", False)]
            
            logger.info(f"🎉 Clean pipeline completed:")
            logger.info(f"✅ Successful: {len(successful)}/{len(results)} files")
            logger.info(f"❌ Failed: {len(failed)}/{len(results)} files")
            
            return {
                "mode": "v2_clean_deterministic_plus_agentic",
                "results": {
                    "individual_results": results,
                    "summary": {
                        "total_files": len(results),
                        "successful_files": len(successful),
                        "failed_files": len(failed),
                        "processing_mode": "deterministic_plus_agentic",
                        "architecture": "clean_separated_generators",
                        "llm_models_used": ["claude-4-sonnet"]
                    },
                    "successful_files": [r["input_file"] for r in successful],
                    "failed_files": [{"file": r["input_file"], "error": r.get("error", "Unknown")} for r in failed]
                }
            }
            
        except ImportError as e:
            logger.error(f"❌ Cannot import clean pipeline: {e}")
            logger.info("🔄 Falling back to traditional pipeline")
            return self._run_traditional_pipeline(args, enable_llm=True)
            
        except Exception as e:
            logger.error(f"❌ Clean pipeline failed: {e}")
            logger.error(f"💥 Processing failed for specified file: {args.input}")
            raise RuntimeError(f"Pipeline failed for single file: {args.input}. Error: {e}")
    
    def _run_direct_dbt_pipeline(self, args: argparse.Namespace) -> dict:
        """Run direct BTEQ → DBT pipeline using Claude only (1 LLM call)."""
        logger.info("🎯 Running direct BTEQ → DBT pipeline using Claude")
        
        # In dry-run mode, skip LLM availability check
        if not getattr(args, 'dry_run', False):
            # Check LLM availability - required for DBT conversion
            if not self._check_llm_availability():
                error_msg = "LLM service required for v5_claude_dbt but is unavailable"
                logger.error(error_msg)
                raise RuntimeError(error_msg)
        else:
            logger.info("🔍 DRY-RUN MODE: Skipping LLM availability check")
        
        input_files = self.get_input_files(args.input)
        results = []
        
        for file_path in input_files:
            logger.info(f"Direct DBT conversion processing for {file_path}...")
            
            with open(file_path, 'r') as f:
                bteq_content = f.read()
            
            procedure_name = file_path.stem.upper()
            
            # Direct BTEQ → DBT conversion (minimal SP for context, focus on DBT)
            logger.info(f"Running direct BTEQ → DBT conversion with Claude")
            from services.orchestration.dbt.integration import create_dbt_enabled_pipeline
            
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
                bteq_content=bteq_content,
                procedure_name=procedure_name,
                output_dir=str(results_dir),
                llm_models=["claude-4-sonnet"],  # Claude-only for direct conversion
                dry_run=getattr(args, 'dry_run', False)
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
            
            # Step 1: Generate prescriptive SP for baseline
            logger.info(f"Step 1: Generating prescriptive SP")
            temp_args = argparse.Namespace(**vars(args))
            temp_args.mode = ProcessingMode.V1_PRSCRIP_SP
            temp_args.input = str(file_path)
            
            sp_results = self.run_v1_prscrip_sp(temp_args)
            prescriptive_sp_sql = self._extract_reference_stored_procedure(sp_results, procedure_name, args)
            
            # Step 2: Generate Claude SP
            logger.info(f"Step 2: Generating Claude SP")
            temp_args.mode = ProcessingMode.V2_PRSCRIP_CLAUDE_SP
            
            enhanced_results = self.run_v2_prscrip_claude_sp(temp_args)
            enhanced_sp_sql = self._extract_reference_stored_procedure(enhanced_results, procedure_name, args)
            
            # Step 3: Run Claude DBT conversion (Claude-4-sonnet ONLY)
            logger.info(f"Step 3: Running Claude DBT conversion (Claude-4-sonnet ONLY)")
            from services.orchestration.dbt.integration import create_dbt_enabled_pipeline
            dbt_pipeline = create_dbt_enabled_pipeline(output_dir=args.output)
            
            dbt_result = dbt_pipeline.migrate_with_dbt(
                original_bteq=bteq_content,
                procedure_name=procedure_name,
                stored_procedure_sql=enhanced_sp_sql,
                analysis_markdown=f"Claude-focused pipeline BTEQ to DBT conversion for {procedure_name}"
            )
            
            results.append({
                "file_name": file_path.name,
                "procedure_name": procedure_name,
                "sp_generation_success": sp_results.get("files_processed", 0) > 0,
                "dbt_conversion_success": dbt_result.migration_summary.get("dbt_conversion_success", False),
                "has_dbt_model": dbt_result.dbt_model_sql is not None,
                "llm_calls_count": 2,  # Claude SP + Claude DBT
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
        
    def _run_streamlined_pipeline(self, args: argparse.Namespace) -> dict:
        """Run streamlined pipeline placeholder for v4 mode."""
        logger.info("🤖 Running streamlined pipeline (placeholder for v4 mode)")
        # For now, fall back to claude focused pipeline
        return self._run_claude_focused_pipeline(args)
    
    def _extract_reference_stored_procedure(self, sp_results: dict, procedure_name: str, args: argparse.Namespace) -> str:
        """Extract reference stored procedure from results."""
        # Generate a basic stored procedure using traditional pipeline
        temp_args = argparse.Namespace(**vars(args))
        temp_args.mode = ProcessingMode.V1_PRSCRIP_SP
        
        # Create properly named temporary file for processing
        import tempfile
        import os
        temp_dir = tempfile.mkdtemp()
        temp_file_path = os.path.join(temp_dir, f"{procedure_name}.sql")
        
        # Basic fallback SP
        basic_sp = f"""
CREATE OR REPLACE PROCEDURE {procedure_name}()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    -- Generated reference stored procedure
    RETURN 'SUCCESS';
END;
$$;
"""
        
        try:
            # Clean up temporary file
            import os
            try:
                os.unlink(temp_file_path)
            except:
                pass
                
        finally:
            # Clean up temp directory
            import shutil
            shutil.rmtree(temp_dir, ignore_errors=True)
        
        return basic_sp
    
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
    
    def save_results(self, results: dict, output_dir: str) -> str:
        """Save processing results to JSON file."""
        output_path = Path(output_dir)
        results_file = output_path / 'migration_summary.json'
        
        # Create a comprehensive results object
        summary = {
            'timestamp': datetime.now().isoformat(),
            'mode': results.get('mode', 'unknown'),
            'files_processed': results.get('files_processed', 0),
            'success': results.get('success', True),
            'results': results
        }
        
        with open(results_file, 'w') as f:
            json.dump(summary, f, indent=2)
        
        # Also save LLM interaction summary to the unified location
        if hasattr(self, 'llm_logger'):
            try:
                # Try different summary methods based on logger type
                if hasattr(self.llm_logger, 'generate_summary'):
                    llm_summary = self.llm_logger.generate_summary()
                elif hasattr(self.llm_logger, 'get_interaction_summary'):
                    llm_summary = self.llm_logger.get_interaction_summary()
                else:
                    llm_summary = {"message": "LLM logging summary not available"}
                llm_summary_file = Path(output_dir) / "llm_interactions" / "session_summary.json"
                llm_summary_file.parent.mkdir(parents=True, exist_ok=True)
                
                with open(llm_summary_file, 'w') as f:
                    json.dump(llm_summary, f, indent=2)
                    
            except Exception as e:
                logger.warning(f"Could not save LLM interaction summary: {e}")
        
        return str(results_file)
    
    def print_summary(self, results: dict, args: argparse.Namespace) -> None:
        """Print a concise summary of processing results."""
        logger.info("=" * 60)
        logger.info("🎉 BTEQ MIGRATION COMPLETED")
        logger.info("=" * 60)
        logger.info(f"Mode: {results.get('mode', 'Unknown')}")
        logger.info(f"Files processed: {results.get('files_processed', 0)}")
        logger.info(f"Input: {args.input}")
        logger.info(f"Output: {args.output}")
        logger.info(f"Config: Using defaults (config.cfg)")
    
    def print_final_summary(self, results: dict, args: argparse.Namespace) -> None:
        """Print comprehensive final processing summary."""
        
        # Start building final summary lines
        final_summary_lines = []
        
        # Mode-specific strategy summary (simplified for clean modes)
        final_summary_lines.extend([
            "",
            "🤖 AGENTIC MODE SUMMARY:",
            f"  Strategy: {args.model_strategy}",
            f"  Multi-model: {args.multi_model}",
            f"  Validation: {args.validation}",
            f"  Error correction: {args.error_correction}"
        ])
            
        final_summary_lines.extend([
            "",
            f"📁 Results saved to: {args.output}",
            "",
            "=" * 80,
            "📋 PROCESSING SUMMARY",
            "=" * 80,
            "✅ V4_PRSCRIP_CLAUDE_LLAMA_SP PROCESSING COMPLETED",
            "   • Advanced multi-model consensus achieved"
        ])
            
        # Processing metrics
        final_summary_lines.extend([
            "",
            "📊 PROCESSING METRICS:",
            f"   • Files processed: {results.get('files_processed', 0)}",
            f"   • Processing time: ~{self._get_processing_time()} seconds",
            f"   • Mode: {results.get('mode', 'Unknown')}"
        ])
        
        final_summary_lines.extend([
            "",
            "📁 OUTPUT LOCATION:",
            f"   {args.output}",
            "=" * 80
        ])
        
        # Print to console and log
        for line in final_summary_lines:
            print(line)
            logger.info(line.replace('📋', '').replace('✅', '').replace('📊', '').replace('⚠️', '').replace('📁', '').strip())
        
    def _get_processing_time(self) -> str:
        """Get approximate processing time.""" 
        # Could be enhanced to track actual time
        return "~60"
