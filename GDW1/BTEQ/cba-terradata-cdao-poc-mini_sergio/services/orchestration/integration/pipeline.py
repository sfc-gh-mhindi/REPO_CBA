"""
Integration layer for Agentic BTEQ Migration with existing pipeline

Provides seamless integration of the agentic framework with the current
BTEQ DCF pipeline, allowing enhanced migration capabilities.
"""

import logging
from typing import Dict, List, Any, Optional
from pathlib import Path
import json

from ..workflows.orchestrator import AgenticOrchestrator, OrchestrationConfig
from ..agents.service import ModelType
# Use absolute imports to fix packaging issues
from services.preprocessing.substitution.service import SubstitutionService
from services.parsing.bteq.parser_service import ParserService

# New clean generator architecture
from services.conversion.stored_proc import (
    SnowflakeSPGeneratorDmst, SnowflakeSPGeneratorAgtc, 
    AgenticSPContext, AgenticGeneratedProcedure
)
from services.conversion.dbt import (
    SnowflakeDBTGeneratorAgtc, DBTConversionContext, DBTConversionResult
)

from utils.logging.llm_logger import get_llm_logger
from services.conversion.postprocessing import PostProcessingService, PostProcessorConfig

logger = logging.getLogger(__name__)


class AgenticMigrationPipeline:
    """Enhanced migration pipeline with agentic capabilities."""
    
    def __init__(self, 
                 config_file: str = "config/database/variable_substitution.cfg",
                 orchestration_config: OrchestrationConfig = None,
                 enable_orchestration: bool = True,
                 llm_log_dir: str = None,
                 dry_run: bool = False,
                 enable_postprocessing: bool = True,
                 postprocessing_config: Optional[PostProcessorConfig] = None):
        """
        Initialize the agentic migration pipeline with clean service architecture.
        
        Args:
            config_file: Path to substitution configuration file
            orchestration_config: Agentic orchestration configuration
            enable_orchestration: Whether to use agentic orchestration
            llm_log_dir: Directory for LLM logging
            enable_postprocessing: Whether to enable post-processing fixes
            postprocessing_config: Custom post-processing configuration
        """
        self.config_file = config_file
        self.enable_orchestration = enable_orchestration
        self.llm_logger = get_llm_logger()
        self.llm_log_dir = llm_log_dir
        self.dry_run = dry_run
        self.enable_postprocessing = enable_postprocessing
        
        # Initialize post-processing service
        if self.enable_postprocessing:
            self.postprocessor = PostProcessingService(postprocessing_config)
            logger.info("✅ Post-processing service initialized")
        else:
            self.postprocessor = None
            logger.info("⚠️  Post-processing service disabled")
        
        # Initialize clean service components
        self.substitution_service = SubstitutionService(config_file)
        self.parser_service = ParserService(enable_advanced_analysis=True)
        
        # Initialize clean generators
        self.sp_generator_dmst = SnowflakeSPGeneratorDmst()
        self.sp_generator_agtc = SnowflakeSPGeneratorAgtc(llm_log_dir=llm_log_dir, dry_run=dry_run)
        self.dbt_generator_agtc = SnowflakeDBTGeneratorAgtc(llm_log_dir=llm_log_dir, dry_run=dry_run)
        
        # Initialize agentic orchestration
        if enable_orchestration:
            self.orchestration_config = orchestration_config or OrchestrationConfig()
            self.orchestrator = AgenticOrchestrator(self.orchestration_config)
        else:
            self.orchestrator = None
    
    def process_bteq_file(self, 
                         input_file: str, 
                         output_directory: Optional[str] = None,
                         procedure_name: Optional[str] = None,
                         use_agentic: Optional[bool] = None) -> Dict[str, Any]:
        """
        Process a single BTEQ file with optional agentic enhancement.
        
        Args:
            input_file: Path to BTEQ file
            output_directory: Output directory for results
            procedure_name: Name for generated procedure
            use_agentic: Override default agentic setting
            
        Returns:
            Processing results including both standard and agentic outputs
        """
        use_agentic = use_agentic if use_agentic is not None else self.enable_agentic
        
        logger.info(f"Processing {input_file} with agentic={use_agentic}")
        
        try:
            # Step 1: Standard pipeline processing
            standard_result = self._run_standard_pipeline(input_file, output_directory)
            
            if not standard_result["success"]:
                return {
                    "success": False,
                    "error": "Standard pipeline failed",
                    "standard_result": standard_result,
                    "agentic_result": None
                }
            
            # Step 2: Agentic enhancement (if enabled)
            agentic_result = None
            if use_agentic:
                agentic_result = self._run_agentic_enhancement(
                    standard_result, procedure_name or self._extract_procedure_name(input_file)
                )
                
                # Save enhanced SP if agentic generation was successful
                if agentic_result and agentic_result.get("success") and "generated_procedure" in agentic_result:
                    self._save_enhanced_sp_file(
                        input_file, 
                        agentic_result["generated_procedure"],
                        procedure_name or self._extract_procedure_name(input_file)
                    )
            
            # Step 3: Compare and select best result
            final_result = self._select_best_result(standard_result, agentic_result)
            
            return {
                "success": True,
                "input_file": input_file,
                "procedure_name": procedure_name,
                "standard_result": standard_result,
                "agentic_result": agentic_result,
                "final_result": final_result,
                "comparison": self._compare_results(standard_result, agentic_result)
            }
            
        except Exception as e:
            logger.error(f"Processing failed for {input_file}: {e}")
            return {
                "success": False,
                "error": str(e),
                "input_file": input_file
            }
    
    def _run_standard_pipeline(self, input_file: str, output_directory: Optional[str]) -> Dict[str, Any]:
        """Run the deterministic BTEQ migration pipeline using clean service architecture."""
        try:
            logger.info(f"🔧 Running deterministic pipeline for: {input_file}")
            
            # Step 1: Variable substitution using clean service
            # Try different encodings to handle extended ASCII characters
            encodings_to_try = ['utf-8', 'windows-1252', 'iso-8859-1', 'utf-8-sig']
            original_content = None
            used_encoding = None
            
            for encoding in encodings_to_try:
                try:
                    with open(input_file, 'r', encoding=encoding) as f:
                        original_content = f.read()
                        used_encoding = encoding
                        break
                except UnicodeDecodeError:
                    continue
            
            if original_content is None:
                raise ValueError(f"Could not read file {input_file} with any supported encoding: {encodings_to_try}")
                
            logger.info(f"✅ Successfully read file using {used_encoding} encoding")
            
            substituted_content, substitution_count = self.substitution_service.substitute_variables(original_content)
            logger.info(f"✅ Variable substitution completed ({substitution_count} substitutions)")
            
            # Step 2: Parse BTEQ content for analysis (optional)
            parse_result = self.parser_service.parse(substituted_content)
            analysis_markdown = self._generate_analysis_markdown(parse_result)
            logger.info("✅ BTEQ parsing and analysis completed")
            
            # Step 3: Generate deterministic stored procedure
            procedure_name = self._extract_procedure_name(input_file)
            
            # Convert ParserServiceOutput back to ParserResult for deterministic generator
            from services.parsing.bteq.tokens import ParserResult, SqlBlock
            sql_blocks = [
                SqlBlock(sql=parsed_sql.original, start_line=parsed_sql.start_line, end_line=parsed_sql.end_line) 
                for parsed_sql in parse_result.sql
            ]
            parser_result_for_dmst = ParserResult(controls=parse_result.controls, sql_blocks=sql_blocks)
            
            deterministic_sp = self.sp_generator_dmst.generate(
                parser_result_for_dmst, 
                procedure_name,
                original_content
            )
            logger.info(f"✅ Deterministic SP generated: {deterministic_sp.name}")
            
            # Save intermediate files for debugging
            self._save_intermediate_files(
                input_file, 
                original_content, 
                substituted_content, 
                parse_result, 
                analysis_markdown, 
                deterministic_sp
            )
            
            # Return deterministic result (clean architecture)
            return {
                "success": True,
                "method": "deterministic",
                "original_bteq": original_content,
                "substituted_bteq": substituted_content,
                "analysis_markdown": analysis_markdown,
                "deterministic_sp": deterministic_sp.sql,
                "procedure_name": deterministic_sp.name,
                "warnings": deterministic_sp.warnings,
                "generation_metadata": {
                    "generator_type": "deterministic", 
                    "uses_parsed_ast": True,
                    "parameters": deterministic_sp.parameters,
                    "error_handling": deterministic_sp.error_handling
                }
            }
            
        except Exception as e:
            logger.error(f"Standard pipeline failed: {e}")
            return {
                "success": False,
                "error": str(e),
                "method": "standard_llm"
            }
    
    def _run_agentic_enhancement(self, 
                                standard_result: Dict[str, Any], 
                                procedure_name: str) -> Dict[str, Any]:
        """Run agentic enhancement using clean agentic SP generator."""
        try:
            logger.info(f"🤖 Running agentic enhancement for {procedure_name}")
            
            # Create agentic context
            agentic_context = AgenticSPContext(
                original_bteq=standard_result["original_bteq"],
                substituted_bteq=standard_result["substituted_bteq"],
                procedure_name=procedure_name,
                reference_deterministic_sp=standard_result.get("deterministic_sp"),
                analysis_notes=standard_result.get("analysis_markdown"),
                complexity_hints=standard_result.get("generation_metadata")
            )
            
            # Generate with multiple models if orchestration enabled
            if self.enable_orchestration and self.orchestrator:
                logger.info("🔄 Using orchestrated multi-model agentic generation")
                # Use orchestrator for full workflow
                agentic_result = self.orchestrator.migrate_bteq(
                    original_bteq=standard_result["original_bteq"],
                    parser_result=standard_result["parse_result"], 
                    target_procedure_name=procedure_name
                )
                
                return {
                    "success": agentic_result["success"],
                    "method": "orchestrated_agentic",
                    "generated_procedure": agentic_result["generated_procedure"],
                    "quality_score": agentic_result["quality_score"],
                    "iterations": agentic_result["iterations"],
                    "errors": agentic_result["errors"],
                    "metadata": agentic_result["metadata"]
                }
                
            else:
                logger.info("🎯 Using direct agentic SP generator")
                # Use clean agentic generator directly
                agentic_results = self.sp_generator_agtc.generate_multi_model(
                    agentic_context, 
                    models=["claude-4-sonnet"]
                )
                
                if agentic_results:
                    # Select best result
                    best_model = max(agentic_results.keys(), 
                                   key=lambda m: agentic_results[m].confidence_score)
                    best_result = agentic_results[best_model]
                    
                    return {
                        "success": True,
                        "method": "direct_agentic",
                        "generated_procedure": best_result.sql,
                        "quality_score": best_result.confidence_score,
                        "model_used": best_result.model_used,
                        "generation_time_ms": best_result.generation_time_ms,
                        "enhancements": best_result.llm_enhancements,
                        "warnings": best_result.warnings
                    }
                else:
                    raise RuntimeError("All agentic models failed")
            
        except Exception as e:
            logger.error(f"❌ Agentic enhancement failed: {e}")
            return {
                "success": False,
                "error": str(e),
                "method": "agentic_enhancement"
            }
    
    def _select_best_result(self, 
                           standard_result: Dict[str, Any], 
                           agentic_result: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        """Select the best result between standard and agentic approaches."""
        
        # If agentic failed or not available, use standard
        if not agentic_result or not agentic_result["success"]:
            return {
                "selected_method": "standard_llm",
                "procedure": standard_result.get("enhanced_procedure", ""),
                "quality_score": standard_result.get("quality_score", 0.0),
                "reason": "Agentic not available or failed"
            }
        
        # Compare quality scores
        standard_quality = standard_result.get("quality_score", 0.0)
        agentic_quality = agentic_result.get("quality_score", 0.0)
        
        if agentic_quality > standard_quality:
            return {
                "selected_method": "agentic_multi_model",
                "procedure": agentic_result["generated_procedure"],
                "quality_score": agentic_quality,
                "reason": f"Higher quality score ({agentic_quality:.2f} vs {standard_quality:.2f})"
            }
        else:
            return {
                "selected_method": "standard_llm",
                "procedure": standard_result["enhanced_procedure"],
                "quality_score": standard_quality,
                "reason": f"Standard quality sufficient ({standard_quality:.2f} vs {agentic_quality:.2f})"
            }
    
    def _compare_results(self, 
                        standard_result: Dict[str, Any], 
                        agentic_result: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        """Compare standard and agentic results."""
        
        comparison = {
            "standard_available": standard_result["success"],
            "agentic_available": agentic_result is not None and agentic_result["success"] if agentic_result else False,
            "quality_difference": 0.0,
            "performance_comparison": {},
            "feature_comparison": {}
        }
        
        if comparison["agentic_available"]:
            standard_quality = standard_result.get("quality_score", 0.0)
            agentic_quality = agentic_result.get("quality_score", 0.0)
            comparison["quality_difference"] = agentic_quality - standard_quality
            
            # Compare features
            comparison["feature_comparison"] = {
                "standard_enhancements": len(standard_result.get("llm_enhancements", [])),
                "agentic_iterations": agentic_result.get("iterations", 0),
                "agentic_validation": agentic_result.get("metadata", {}).get("validation_passed", False),
                "agentic_correction": agentic_result.get("metadata", {}).get("correction_applied", False)
            }
        
        return comparison
    
    def _save_intermediate_files(self, 
                               input_file: str, 
                               original_content: str, 
                               substituted_content: str, 
                               parse_result, 
                               analysis_markdown: str, 
                               deterministic_sp) -> None:
        """Save intermediate processing files for debugging."""
        import json
        from pathlib import Path
        
        # Create base filename from input file
        input_path = Path(input_file)
        base_name = input_path.stem
        
        # Use llm_log_dir as base output directory if available
        if hasattr(self, 'llm_log_dir') and self.llm_log_dir:
            output_base = Path(self.llm_log_dir).parent
        else:
            output_base = Path("output/debug")
            
        # Create directory structure
        substituted_dir = output_base / "substituted"
        parsed_dir = output_base / "parsed" 
        snowflake_sp_dir = output_base / "snowflake_sp"
        analysis_dir = output_base / "analysis"
        
        for dir_path in [substituted_dir, parsed_dir, snowflake_sp_dir, analysis_dir]:
            dir_path.mkdir(parents=True, exist_ok=True)
        
        # 1. Save substituted BTEQ file
        substituted_file = substituted_dir / f"{base_name}_substituted.bteq"
        with open(substituted_file, 'w', encoding='utf-8') as f:
            f.write(substituted_content)
        
        # 2. Save parser output as JSON
        parser_file = parsed_dir / f"{base_name}_parsed.json"
        # Convert ParsedSql objects to dict for JSON serialization
        parser_data = {
            "controls": [{"type": str(c.type), "raw": c.raw, "line_no": c.line_no} for c in parse_result.controls],
            "sql_blocks": [
                {
                    "original": sql.original,
                    "snowflake_sql": sql.snowflake_sql, 
                    "start_line": sql.start_line,
                    "end_line": sql.end_line,
                    "error": sql.error
                }
                for sql in parse_result.sql
            ]
        }
        with open(parser_file, 'w', encoding='utf-8') as f:
            json.dump(parser_data, f, indent=2)
        
        # 3. Save generated stored procedure with lowercase filename
        sp_file = snowflake_sp_dir / f"{deterministic_sp.name.lower()}_dtrmstc.sql"
        with open(sp_file, 'w', encoding='utf-8') as f:
            f.write(deterministic_sp.sql)
        
        # 4. Save analysis markdown
        analysis_file = analysis_dir / f"{base_name}_analysis.md"
        with open(analysis_file, 'w', encoding='utf-8') as f:
            f.write(analysis_markdown)
        
        logger.info(f"💾 Saved intermediate files to {output_base}")
        logger.info(f"   - Substituted: {substituted_file}")
        logger.info(f"   - Parsed: {parser_file}") 
        logger.info(f"   - Deterministic SP: {sp_file}")
        logger.info(f"   - Analysis: {analysis_file}")
    
    def _save_enhanced_sp_file(self, 
                              input_file: str, 
                              enhanced_sp_sql: str,
                              procedure_name: str) -> None:
        """Save the agentic-enhanced stored procedure to a file."""
        
        # Use the same directory logic as _save_intermediate_files
        if hasattr(self, 'llm_log_dir') and self.llm_log_dir:
            output_base = Path(self.llm_log_dir).parent
        else:
            output_base = Path("output/debug")
        
        # Create enhanced SP directory (same as deterministic SP)
        enhanced_sp_dir = Path(output_base) / "snowflake_sp"
        enhanced_sp_dir.mkdir(parents=True, exist_ok=True)
        
        # Save enhanced SP without suffix (becomes the main file) with lowercase filename
        enhanced_sp_file = enhanced_sp_dir / f"{procedure_name.lower()}.sql"
        with open(enhanced_sp_file, 'w', encoding='utf-8') as f:
            f.write(enhanced_sp_sql)
        
        logger.info(f"✨ Saved enhanced SP file: {enhanced_sp_file}")
        
        # Apply post-processing fixes if enabled
        if self.enable_postprocessing and self.postprocessor:
            logger.info("🔧 Applying post-processing fixes...")
            try:
                post_result = self.postprocessor.process_file(str(enhanced_sp_file))
                if post_result.success and post_result.total_changes > 0:
                    logger.info(f"✅ Post-processing applied {post_result.total_changes} fixes:")
                    for fix in post_result.fixes_applied:
                        if fix.success and fix.changes_made > 0:
                            logger.info(f"   - {fix.fix_type.value}: {fix.changes_made} changes")
                            for pattern in fix.applied_patterns:
                                logger.debug(f"     {pattern}")
                elif post_result.success:
                    logger.info("✅ Post-processing completed - no fixes needed")
                else:
                    logger.warning(f"⚠️ Post-processing failed: {post_result.error_message}")
            except Exception as e:
                logger.error(f"❌ Post-processing error: {e}")
        
        logger.info(f"   - Main SP: {enhanced_sp_file}")
    
    def _generate_analysis_markdown(self, parse_result) -> str:
        """Generate analysis markdown from parse result."""
        # Simplified analysis generation (ParserServiceOutput has 'sql' not 'sql_blocks')
        controls = len(parse_result.controls) if hasattr(parse_result, 'controls') else 0
        sql_blocks = len(parse_result.sql) if hasattr(parse_result, 'sql') else 0
        
        return f"""# BTEQ Analysis
        
## Overview
- Control Statements: {controls}
- SQL Blocks: {sql_blocks}
- Complexity: Medium

## Recommendations
- Standard migration approach suitable
- Consider agentic enhancement for complex patterns
"""
    
    def _extract_procedure_name(self, input_file: str) -> str:
        """Extract procedure name from file path."""
        return Path(input_file).stem.upper() + "_PROC"
    
    def batch_process(self, 
                     input_directory: str,
                     output_directory: str,
                     file_pattern: str = "*.bteq",
                     use_agentic: bool = True) -> Dict[str, Any]:
        """
        Process multiple BTEQ files with agentic enhancement.
        
        Args:
            input_directory: Directory containing BTEQ files
            output_directory: Output directory for results
            file_pattern: File pattern to match
            use_agentic: Whether to use agentic enhancement
            
        Returns:
            Batch processing results with summary statistics
        """
        input_path = Path(input_directory)
        output_path = Path(output_directory)
        output_path.mkdir(parents=True, exist_ok=True)
        
        # Find BTEQ files
        bteq_files = list(input_path.glob(file_pattern))
        
        logger.info(f"Processing {len(bteq_files)} files with agentic={use_agentic}")
        
        results = {
            "total_files": len(bteq_files),
            "successful_files": 0,
            "failed_files": 0,
            "agentic_improvements": 0,
            "quality_improvements": 0.0,
            "file_results": {},
            "summary_statistics": {}
        }
        
        for bteq_file in bteq_files:
            try:
                # Process individual file
                file_result = self.process_bteq_file(
                    str(bteq_file),
                    str(output_path),
                    use_agentic=use_agentic
                )
                
                file_name = bteq_file.name
                results["file_results"][file_name] = file_result
                
                if file_result["success"]:
                    results["successful_files"] += 1
                    
                    # Check for agentic improvements
                    comparison = file_result.get("comparison", {})
                    if comparison.get("quality_difference", 0) > 0:
                        results["agentic_improvements"] += 1
                        results["quality_improvements"] += comparison["quality_difference"]
                    
                    # Save final procedure
                    final_procedure = file_result["final_result"]["procedure"]
                    procedure_file = output_path / f"{bteq_file.stem}_procedure.sql"
                    with open(procedure_file, 'w', encoding='utf-8') as f:
                        f.write(final_procedure)
                    
                    # Save metadata
                    metadata_file = output_path / f"{bteq_file.stem}_metadata.json"
                    with open(metadata_file, 'w', encoding='utf-8') as f:
                        json.dump(file_result, f, indent=2, default=str)
                
                else:
                    results["failed_files"] += 1
                    
            except Exception as e:
                logger.error(f"Failed to process {bteq_file}: {e}")
                results["failed_files"] += 1
                results["file_results"][bteq_file.name] = {
                    "success": False,
                    "error": str(e)
                }
        
        # Calculate summary statistics
        if results["successful_files"] > 0:
            results["summary_statistics"] = {
                "success_rate": results["successful_files"] / results["total_files"],
                "agentic_improvement_rate": results["agentic_improvements"] / results["successful_files"],
                "average_quality_improvement": results["quality_improvements"] / max(results["agentic_improvements"], 1)
            }
        
        # Save batch summary
        summary_file = output_path / "batch_summary.json"
        with open(summary_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, default=str)
        
        logger.info(f"Batch processing complete: {results['successful_files']}/{results['total_files']} successful")
        
        return results


class AgenticValidationSuite:
    """Validation suite for comparing standard vs agentic approaches."""
    
    def __init__(self, pipeline: AgenticMigrationPipeline):
        self.pipeline = pipeline
        self.llm_logger = get_llm_logger()
    
    def validate_agentic_improvements(self, 
                                    test_files: List[str],
                                    output_directory: str) -> Dict[str, Any]:
        """
        Validate that agentic approach provides improvements over standard approach.
        
        Args:
            test_files: List of BTEQ test files
            output_directory: Directory for validation results
            
        Returns:
            Validation results and statistics
        """
        output_path = Path(output_directory)
        output_path.mkdir(parents=True, exist_ok=True)
        
        validation_results = {
            "test_files_count": len(test_files),
            "improvements_found": 0,
            "quality_improvements": [],
            "feature_improvements": [],
            "performance_comparisons": [],
            "detailed_results": {}
        }
        
        for test_file in test_files:
            logger.info(f"Validating improvements for {test_file}")
            
            try:
                # Run both standard and agentic
                result = self.pipeline.process_bteq_file(
                    test_file, 
                    str(output_path), 
                    use_agentic=True
                )
                
                if result["success"]:
                    comparison = result["comparison"]
                    
                    # Check for improvements
                    quality_diff = comparison.get("quality_difference", 0)
                    if quality_diff > 0:
                        validation_results["improvements_found"] += 1
                        validation_results["quality_improvements"].append(quality_diff)
                    
                    # Check feature improvements
                    features = comparison.get("feature_comparison", {})
                    if features.get("agentic_validation", False) or features.get("agentic_correction", False):
                        validation_results["feature_improvements"].append({
                            "file": test_file,
                            "validation": features.get("agentic_validation", False),
                            "correction": features.get("agentic_correction", False),
                            "iterations": features.get("agentic_iterations", 0)
                        })
                    
                    validation_results["detailed_results"][Path(test_file).name] = result
                
            except Exception as e:
                logger.error(f"Validation failed for {test_file}: {e}")
                validation_results["detailed_results"][Path(test_file).name] = {
                    "error": str(e)
                }
        
        # Calculate summary statistics
        if validation_results["quality_improvements"]:
            validation_results["average_quality_improvement"] = sum(validation_results["quality_improvements"]) / len(validation_results["quality_improvements"])
            validation_results["max_quality_improvement"] = max(validation_results["quality_improvements"])
            validation_results["improvement_rate"] = validation_results["improvements_found"] / validation_results["test_files_count"]
        
        # Save validation report
        report_file = output_path / "agentic_validation_report.json"
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(validation_results, f, indent=2, default=str)
        
        return validation_results


# Factory functions
def create_agentic_pipeline(config_file: str = "config.cfg", 
                           enable_agentic: bool = True) -> AgenticMigrationPipeline:
    """Create an agentic migration pipeline."""
    return AgenticMigrationPipeline(config_file, enable_agentic=enable_agentic)

def create_validation_suite(pipeline: AgenticMigrationPipeline) -> AgenticValidationSuite:
    """Create a validation suite for the agentic pipeline."""
    return AgenticValidationSuite(pipeline)
