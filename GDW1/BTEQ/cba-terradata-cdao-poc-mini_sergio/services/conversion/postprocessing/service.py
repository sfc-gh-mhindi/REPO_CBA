"""
Post-processing service for handling recurring Snowflake syntax issues.

This service provides a generic framework for applying common fixes to generated
stored procedures, such as dollar delimiter issues, data type conversions, and
ICEBERG table syntax corrections.
"""

import logging
import time
from pathlib import Path
from typing import List, Optional, Dict, Any
import shutil

from .models import (
    PostProcessorConfig, PostProcessingResult, FixResult, FixType, CustomFix
)
from .fixes import create_fix, BaseFix

logger = logging.getLogger(__name__)


class PostProcessingService:
    """
    Service for post-processing generated Snowflake stored procedures.
    
    Handles common syntax issues that occur after AI-enhanced code generation,
    providing a modular and configurable approach to fix recurring problems.
    """
    
    def __init__(self, config: Optional[PostProcessorConfig] = None):
        """
        Initialize the post-processing service.
        
        Args:
            config: Configuration for the post-processor. If None, uses defaults.
        """
        self.config = config or PostProcessorConfig()
        self.fixes: List[BaseFix] = []
        self._initialize_fixes()
        
        logger.info(f"PostProcessingService initialized with {len(self.fixes)} fixes")
        logger.debug(f"Enabled fix types: {[fix.fix_type.value for fix in self.fixes]}")
    
    def _initialize_fixes(self):
        """Initialize all configured fixes."""
        self.fixes.clear()
        
        # Create fixes for all enabled types
        for fix_type in self.config.enabled_fixes:
            try:
                fix_instance = create_fix(fix_type, self.config)
                self.fixes.append(fix_instance)
                logger.debug(f"Initialized fix: {fix_type.value}")
            except Exception as e:
                logger.error(f"Failed to initialize fix {fix_type.value}: {e}")
    
    def add_custom_fix(self, custom_fix: CustomFix):
        """
        Add a custom fix to the processor.
        
        Args:
            custom_fix: Custom fix definition
        """
        # Find existing custom fix handler or create new one
        custom_fix_handler = None
        for fix in self.fixes:
            if fix.fix_type == FixType.CUSTOM:
                custom_fix_handler = fix
                break
        
        if not custom_fix_handler:
            custom_fix_handler = create_fix(FixType.CUSTOM, self.config, custom_fixes=[])
            self.fixes.append(custom_fix_handler)
        
        custom_fix_handler.custom_fixes.append(custom_fix)
        logger.info(f"Added custom fix: {custom_fix.name}")
    
    def process_file(self, file_path: str) -> PostProcessingResult:
        """
        Process a stored procedure file and apply all configured fixes.
        
        Args:
            file_path: Path to the stored procedure file
            
        Returns:
            PostProcessingResult with details of all applied fixes
        """
        start_time = time.time()
        file_path = Path(file_path)
        
        logger.info(f"Processing file: {file_path}")
        
        try:
            # Read the file
            with open(file_path, 'r', encoding='utf-8') as f:
                original_content = f.read()
            
            # Create backup if enabled
            backup_created = False
            if self.config.create_backup:
                backup_path = file_path.with_suffix(f"{file_path.suffix}.backup")
                shutil.copy2(file_path, backup_path)
                backup_created = True
                logger.debug(f"Created backup: {backup_path}")
            
            # Process the content (including file operations)
            result = self.process_file_content(original_content, str(file_path))
            result.backup_created = backup_created
            
            # Write back the processed content if changes were made
            if result.total_changes > 0:
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(result.processed_content)
                logger.info(f"Applied {result.total_changes} fixes to {file_path}")
            else:
                logger.info(f"No fixes needed for {file_path}")
            
            # Calculate processing time
            processing_time = int((time.time() - start_time) * 1000)
            result.processing_time_ms = processing_time
            
            return result
            
        except Exception as e:
            error_msg = f"Failed to process file {file_path}: {e}"
            logger.error(error_msg)
            return PostProcessingResult(
                success=False,
                original_content="",
                processed_content="",
                fixes_applied=[],
                error_message=error_msg,
                processing_time_ms=int((time.time() - start_time) * 1000)
            )
    
    def process_file_content(self, content: str, file_path: Optional[str] = None) -> PostProcessingResult:
        """
        Process content string with optional file path for file-based operations.
        
        Args:
            content: The content to process
            file_path: Optional file path for file-based fixes
            
        Returns:
            PostProcessingResult with details of all applied fixes
        """
        start_time = time.time()
        original_content = content
        processed_content = content
        fixes_applied: List[FixResult] = []
        
        logger.debug(f"Processing content ({len(content)} characters)")
        if file_path:
            logger.debug(f"File path provided for fixes: {file_path}")
        
        try:
            # Apply fixes sequentially with file path
            processed_content = self._apply_fixes_sequentially(
                processed_content, fixes_applied, file_path
            )
            
            # Validate syntax if enabled
            if self.config.validate_syntax_after_fixes:
                validation_result = self._validate_syntax(processed_content)
                if not validation_result:
                    logger.warning("Syntax validation failed after applying fixes")
            
            total_changes = sum(fix.changes_made for fix in fixes_applied)
            processing_time = int((time.time() - start_time) * 1000)
            
            logger.info(f"Applied {len(fixes_applied)} fix types with {total_changes} total changes")
            
            return PostProcessingResult(
                success=True,
                original_content=original_content,
                processed_content=processed_content,
                fixes_applied=fixes_applied,
                total_changes=total_changes,
                processing_time_ms=processing_time
            )
            
        except Exception as e:
            error_msg = f"Failed to process content: {e}"
            logger.error(error_msg)
            return PostProcessingResult(
                success=False,
                original_content=original_content,
                processed_content=original_content,  # Return original on error
                fixes_applied=fixes_applied,
                error_message=error_msg,
                processing_time_ms=int((time.time() - start_time) * 1000)
            )
    
    def process_content(self, content: str) -> PostProcessingResult:
        """
        Process content string and apply all configured fixes (without file operations).
        
        Args:
            content: The content to process
            
        Returns:
            PostProcessingResult with details of all applied fixes
        """
        return self.process_file_content(content, None)
    
    def _apply_fixes_sequentially(self, content: str, fixes_applied: List[FixResult], file_path: Optional[str] = None) -> str:
        """Apply all fixes sequentially."""
        current_content = content
        
        for fix in self.fixes:
            try:
                # Set file path for file-based fixes
                if hasattr(fix, 'set_target_file') and file_path:
                    fix.set_target_file(file_path)
                
                # Check if fix can be applied
                if not fix.can_apply(current_content):
                    logger.debug(f"Skipping {fix.fix_type.value} fix - not applicable")
                    continue
                
                # Apply the fix
                logger.debug(f"Applying {fix.fix_type.value} fix")
                fix_result = fix.apply(current_content)
                fixes_applied.append(fix_result)
                
                if fix_result.success and fix_result.changes_made > 0:
                    current_content = fix_result.modified_content
                    logger.info(
                        f"{fix.fix_type.value} fix applied: {fix_result.changes_made} changes"
                    )
                    
                    if self.config.log_all_changes:
                        for pattern in fix_result.applied_patterns:
                            logger.debug(f"  {pattern}")
                elif not fix_result.success:
                    logger.error(f"{fix.fix_type.value} fix failed: {fix_result.error_message}")
                
            except Exception as e:
                logger.error(f"Error applying {fix.fix_type.value} fix: {e}")
                # Create error result
                error_result = FixResult(
                    success=False,
                    fix_type=fix.fix_type,
                    changes_made=0,
                    error_message=str(e)
                )
                fixes_applied.append(error_result)
        
        return current_content
    
    def _validate_syntax(self, content: str) -> bool:
        """
        Validate SQL syntax (basic validation).
        
        Args:
            content: SQL content to validate
            
        Returns:
            True if syntax appears valid, False otherwise
        """
        # Basic validation - check for balanced delimiters
        try:
            dollar_count = content.count('$$')
            if dollar_count % 2 != 0:
                logger.warning("Unbalanced $$ delimiters detected")
                return False
            
            paren_count = content.count('(') - content.count(')')
            if paren_count != 0:
                logger.warning("Unbalanced parentheses detected")
                return False
            
            # Check for required stored procedure elements
            if 'CREATE' not in content.upper() or 'PROCEDURE' not in content.upper():
                logger.warning("Content does not appear to contain a stored procedure")
                return False
            
            return True
            
        except Exception as e:
            logger.error(f"Syntax validation error: {e}")
            return False
    
    def get_applicable_fixes(self, content: str) -> List[FixType]:
        """
        Get list of fix types that are applicable to the given content.
        
        Args:
            content: Content to check
            
        Returns:
            List of applicable fix types
        """
        applicable_fixes = []
        
        for fix in self.fixes:
            try:
                if fix.can_apply(content):
                    applicable_fixes.append(fix.fix_type)
            except Exception as e:
                logger.error(f"Error checking applicability of {fix.fix_type.value}: {e}")
        
        return applicable_fixes
    
    def preview_fixes(self, content: str) -> Dict[str, Any]:
        """
        Preview what fixes would be applied without actually applying them.
        
        Args:
            content: Content to preview fixes for
            
        Returns:
            Dictionary with preview information
        """
        applicable_fixes = self.get_applicable_fixes(content)
        
        preview = {
            "content_length": len(content),
            "applicable_fixes": [fix.value for fix in applicable_fixes],
            "fix_count": len(applicable_fixes),
            "would_modify": len(applicable_fixes) > 0
        }
        
        # Get detailed preview for each applicable fix
        fix_previews = {}
        for fix in self.fixes:
            if fix.fix_type in applicable_fixes:
                try:
                    # This is a dry run - we don't actually modify anything
                    preview_result = fix.apply(content)
                    fix_previews[fix.fix_type.value] = {
                        "changes_estimated": preview_result.changes_made,
                        "patterns": preview_result.applied_patterns
                    }
                except Exception as e:
                    fix_previews[fix.fix_type.value] = {
                        "error": str(e)
                    }
        
        preview["fix_details"] = fix_previews
        return preview
