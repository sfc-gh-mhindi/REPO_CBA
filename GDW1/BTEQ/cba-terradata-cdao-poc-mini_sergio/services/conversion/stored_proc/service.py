"""Generator service orchestrating BTEQ to Snowflake conversion."""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional, Dict, Any

# Use absolute imports to fix packaging issues
try:
    from services.parsing.bteq.tokens import ParserResult
except ImportError:
    # Fallback for when running as part of package
    from services.parsing.bteq.tokens import ParserResult
from .generator import SnowflakeSPGenerator, GeneratedProcedure, extract_procedure_name_from_bteq


@dataclass
class GenerationResult:
    """Complete result of BTEQ to Snowflake generation."""
    procedure: GeneratedProcedure
    parser_result: ParserResult
    original_bteq: str
    generation_metadata: Dict[str, Any]


class GeneratorService:
    """Main service for generating Snowflake code from BTEQ parser output."""
    
    def __init__(self, 
                 enable_logging: bool = True,
                 log_level: str = "INFO"):
        """Initialize generator service.
        
        Args:
            enable_logging: Enable detailed logging
            log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
        """
        self.sp_generator = SnowflakeSPGenerator()
        
        if enable_logging:
            logging.basicConfig(level=getattr(logging, log_level))
        self.logger = logging.getLogger(__name__)
        
    def generate(self, 
                 parser_result: ParserResult,
                 original_bteq: str,
                 procedure_name: Optional[str] = None,
                 options: Optional[Dict[str, Any]] = None) -> GenerationResult:
        """Generate Snowflake stored procedure from BTEQ parser result.
        
        Args:
            parser_result: Output from BTEQ parser/lexer
            original_bteq: Original BTEQ script content
            procedure_name: Optional procedure name override
            options: Generation options (future extensibility)
            
        Returns:
            Complete generation result with metadata
        """
        options = options or {}
        
        self.logger.info("Starting BTEQ to Snowflake generation")
        
        # Extract procedure name if not provided
        if not procedure_name:
            procedure_name = extract_procedure_name_from_bteq(original_bteq)
            self.logger.info(f"Auto-generated procedure name: {procedure_name}")
        
        # Generate stored procedure
        self.logger.info(f"Generating stored procedure: {procedure_name}")
        procedure = self.sp_generator.generate(
            parser_result=parser_result,
            procedure_name=procedure_name,
            original_bteq=original_bteq
        )
        
        # Collect generation metadata
        generation_metadata = {
            "generation_timestamp": None,  # Set by caller if needed
            "parser_stats": {
                "control_statements": len(parser_result.controls),
                "sql_blocks": len(parser_result.sql_blocks),
                "control_types": list(set(c.type.name for c in parser_result.controls))
            },
            "generation_stats": {
                "procedure_name": procedure.name,
                "parameter_count": len(procedure.parameters),
                "warning_count": len(procedure.warnings),
                "error_handling_features": len(procedure.error_handling)
            },
            "options_used": options
        }
        
        self.logger.info(f"Generation completed. Warnings: {len(procedure.warnings)}")
        
        if procedure.warnings:
            self.logger.warning("Generation warnings:")
            for warning in procedure.warnings:
                self.logger.warning(f"  - {warning}")
        
        return GenerationResult(
            procedure=procedure,
            parser_result=parser_result,
            original_bteq=original_bteq,
            generation_metadata=generation_metadata
        )
    
    def generate_batch(self, 
                      bteq_files: Dict[str, str],
                      options: Optional[Dict[str, Any]] = None) -> Dict[str, GenerationResult]:
        """Generate stored procedures for multiple BTEQ files.
        
        Args:
            bteq_files: Dict mapping file paths to BTEQ content
            options: Generation options applied to all files
            
        Returns:
            Dict mapping file paths to generation results
        """
        # Import here to avoid circular imports
        # Use absolute imports to fix packaging issues
        try:
            from services.parsing.bteq.lexer import lex_bteq
        except ImportError:
            # Fallback for when running as part of package
            from services.parsing.bteq.lexer import lex_bteq
        
        results = {}
        options = options or {}
        
        self.logger.info(f"Starting batch generation for {len(bteq_files)} files")
        
        for file_path, bteq_content in bteq_files.items():
            try:
                self.logger.info(f"Processing file: {file_path}")
                
                # Parse BTEQ content
                parser_result = lex_bteq(bteq_content)
                
                # Extract base name for procedure
                import os
                base_name = os.path.splitext(os.path.basename(file_path))[0]
                procedure_name = f"PROC_{base_name.upper()}"
                
                # Generate
                result = self.generate(
                    parser_result=parser_result,
                    original_bteq=bteq_content,
                    procedure_name=procedure_name,
                    options=options
                )
                
                results[file_path] = result
                self.logger.info(f"Successfully processed: {file_path}")
                
            except Exception as e:
                self.logger.error(f"Failed to process {file_path}: {str(e)}")
                # Continue processing other files
                continue
        
        self.logger.info(f"Batch generation completed. Processed {len(results)}/{len(bteq_files)} files")
        return results
    
    def validate_generation(self, result: GenerationResult) -> Dict[str, Any]:
        """Validate generated stored procedure for common issues.
        
        Args:
            result: Generation result to validate
            
        Returns:
            Validation report with issues and recommendations
        """
        validation_report = {
            "valid": True,
            "issues": [],
            "recommendations": [],
            "summary": {}
        }
        
        procedure = result.procedure
        
        # Check for common issues
        
        # 1. Too many warnings
        if len(procedure.warnings) > 10:
            validation_report["issues"].append(
                f"High warning count ({len(procedure.warnings)}). Manual review recommended."
            )
            validation_report["valid"] = False
        
        # 2. Missing error handling for certain patterns
        control_types = set(c.type.name for c in result.parser_result.controls)
        if "CALL_SP" in control_types and "CALL_SP error checking implemented" not in procedure.error_handling:
            validation_report["issues"].append(
                "CALL statements found but error checking may be incomplete"
            )
        
        # 3. Unsupported features
        unsupported_controls = ["OS_CMD", "IMPORT", "EXPORT"]
        found_unsupported = control_types.intersection(unsupported_controls)
        if found_unsupported:
            validation_report["recommendations"].append(
                f"Manual review required for: {', '.join(found_unsupported)}"
            )
        
        # 4. SQL complexity
        sql_block_count = len(result.parser_result.sql_blocks)
        if sql_block_count > 20:
            validation_report["recommendations"].append(
                f"High SQL block count ({sql_block_count}). Consider breaking into multiple procedures."
            )
        
        # 5. Check procedure SQL for basic syntax issues
        sql_lines = procedure.sql.split('\n')
        for i, line in enumerate(sql_lines, 1):
            if 'GOTO' in line.upper() and 'goto' not in line.lower():
                # Snowflake GOTO should be lowercase in most contexts
                validation_report["issues"].append(f"Line {i}: Check GOTO syntax case")
        
        validation_report["summary"] = {
            "total_issues": len(validation_report["issues"]),
            "total_recommendations": len(validation_report["recommendations"]),
            "requires_manual_review": len(validation_report["issues"]) > 0 or len(found_unsupported) > 0
        }
        
        return validation_report
