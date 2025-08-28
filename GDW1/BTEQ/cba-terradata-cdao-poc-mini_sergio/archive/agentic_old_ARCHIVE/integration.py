"""
Integration layer for Agentic BTEQ Migration with existing pipeline

Provides seamless integration of the agentic framework with the current
BTEQ DCF pipeline, allowing enhanced migration capabilities.
"""

import logging
from typing import Dict, List, Any, Optional
from pathlib import Path
import json

from .orchestrator import AgenticOrchestrator, OrchestrationConfig
from .agents import ModelType
# Use absolute imports to fix packaging issues
from services.migration.pipelines.substitution import SubstitutionPipeline
from services.parsing.bteq.parser_service import ParserService
from generator.llm_enhanced_generator import LLMEnhancedGenerator
from utils.logging.llm_logger import get_llm_logger

logger = logging.getLogger(__name__)


class AgenticMigrationPipeline:
    """Enhanced migration pipeline with agentic capabilities."""
    
    def __init__(self, 
                 config_file: str = "config.cfg",
                 orchestration_config: OrchestrationConfig = None,
                 enable_agentic: bool = True):
        """
        Initialize the agentic migration pipeline.
        
        Args:
            config_file: Path to configuration file
            orchestration_config: Agentic orchestration configuration
            enable_agentic: Whether to use agentic features
        """
        self.config_file = config_file
        self.enable_agentic = enable_agentic
        self.llm_logger = get_llm_logger()
        
        # Initialize existing pipeline components
        self.substitution_pipeline = SubstitutionPipeline(config_file)
        self.parser_service = ParserService(enable_advanced_analysis=True)
        self.standard_generator = LLMEnhancedGenerator()
        
        # Initialize agentic components
        if enable_agentic:
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
            if use_agentic and self.orchestrator:
                agentic_result = self._run_agentic_enhancement(
                    standard_result, procedure_name or self._extract_procedure_name(input_file)
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
        """Run the standard BTEQ migration pipeline."""
        try:
            # Variable substitution
            with open(input_file, 'r', encoding='utf-8') as f:
                original_content = f.read()
            
            substituted_content = self.substitution_pipeline.substitution_service.substitute_variables(
                original_content, self.substitution_pipeline.variable_mappings
            )
            
            # Parse BTEQ content
            parse_result = self.parser_service.parse(substituted_content)
            
            # Generate analysis
            analysis_markdown = self._generate_analysis_markdown(parse_result)
            
            # Generate standard procedure
            basic_procedure = self.standard_generator.basic_generator.generate(
                parse_result, 
                self._extract_procedure_name(input_file),
                original_content
            )
            
            # Generate enhanced procedure
            # Use absolute imports to fix packaging issues
            try:
                from generator.llm_enhanced_generator import create_llm_generation_context
            except ImportError:
                # Fallback for when running as part of package
                from generator.llm_enhanced_generator import create_llm_generation_context
            context = create_llm_generation_context(
                original_bteq=original_content,
                analysis_markdown=analysis_markdown,
                basic_procedure=basic_procedure.sql,
                procedure_name=self._extract_procedure_name(input_file),
                parser_result=parse_result
            )
            
            enhanced_result = self.standard_generator.generate_enhanced(context)
            
            return {
                "success": True,
                "method": "standard_llm",
                "original_bteq": original_content,
                "substituted_bteq": substituted_content,
                "parse_result": parse_result,
                "analysis_markdown": analysis_markdown,
                "basic_procedure": basic_procedure.sql,
                "enhanced_procedure": enhanced_result.sql,
                "quality_score": enhanced_result.quality_score,
                "llm_enhancements": enhanced_result.llm_enhancements,
                "warnings": enhanced_result.warnings
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
        """Run agentic enhancement on the standard result."""
        try:
            logger.info(f"Running agentic enhancement for {procedure_name}")
            
            # Execute agentic migration
            agentic_result = self.orchestrator.migrate_bteq(
                original_bteq=standard_result["original_bteq"],
                parser_result=standard_result["parse_result"],
                target_procedure_name=procedure_name
            )
            
            return {
                "success": agentic_result["success"],
                "method": "agentic_multi_model",
                "generated_procedure": agentic_result["generated_procedure"],
                "quality_score": agentic_result["quality_score"],
                "iterations": agentic_result["iterations"],
                "errors": agentic_result["errors"],
                "metadata": agentic_result["metadata"]
            }
            
        except Exception as e:
            logger.error(f"Agentic enhancement failed: {e}")
            return {
                "success": False,
                "error": str(e),
                "method": "agentic_multi_model"
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
    
    def _generate_analysis_markdown(self, parse_result) -> str:
        """Generate analysis markdown from parse result."""
        # Simplified analysis generation
        controls = len(parse_result.controls) if hasattr(parse_result, 'controls') else 0
        sql_blocks = len(parse_result.sql_blocks) if hasattr(parse_result, 'sql_blocks') else 0
        
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
