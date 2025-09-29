"""
Individual fix implementations for the post-processing service.
"""

import re
import logging
from pathlib import Path
from typing import List, Dict, Tuple, Optional
from .models import FixType, FixResult, PostProcessorConfig, CustomFix

logger = logging.getLogger(__name__)


class BaseFix:
    """Base class for all fixes."""
    
    def __init__(self, config: PostProcessorConfig):
        self.config = config
        self.fix_type = FixType.CUSTOM
        
    def apply(self, content: str) -> FixResult:
        """Apply the fix to the content. Must be implemented by subclasses."""
        raise NotImplementedError("Subclasses must implement apply method")
        
    def can_apply(self, content: str) -> bool:
        """Check if this fix can be applied to the content."""
        return True
        
    def _count_changes(self, original: str, modified: str) -> int:
        """Count the number of changes made."""
        return 0 if original == modified else 1


class DollarDelimiterFix(BaseFix):
    """Fix dollar delimiter syntax issues ($DOLLAR$ → $$)."""
    
    def __init__(self, config: PostProcessorConfig):
        super().__init__(config)
        self.fix_type = FixType.DOLLAR_DELIMITER
        
    def apply(self, content: str) -> FixResult:
        """Apply dollar delimiter fixes."""
        original_content = content
        modified_content = content
        changes_made = 0
        applied_patterns = []
        
        try:
            # Apply each dollar delimiter pattern
            for pattern, replacement in self.config.dollar_delimiter_patterns.items():
                if re.search(pattern, modified_content, re.IGNORECASE):
                    before_content = modified_content
                    modified_content = re.sub(pattern, replacement, modified_content, flags=re.IGNORECASE)
                    
                    if before_content != modified_content:
                        pattern_changes = len(re.findall(pattern, before_content, re.IGNORECASE))
                        changes_made += pattern_changes
                        applied_patterns.append(f"{pattern} → {replacement} ({pattern_changes} changes)")
                        logger.debug(f"Applied dollar delimiter fix: {pattern} → {replacement}")
            
            return FixResult(
                success=True,
                fix_type=self.fix_type,
                changes_made=changes_made,
                original_content=original_content,
                modified_content=modified_content,
                applied_patterns=applied_patterns
            )
            
        except Exception as e:
            logger.error(f"Dollar delimiter fix failed: {e}")
            return FixResult(
                success=False,
                fix_type=self.fix_type,
                changes_made=0,
                error_message=str(e)
            )
    
    def can_apply(self, content: str) -> bool:
        """Check if dollar delimiter fixes are needed."""
        for pattern in self.config.dollar_delimiter_patterns.keys():
            if re.search(pattern, content, re.IGNORECASE):
                return True
        return False


class GetDiagnosticsFix(BaseFix):
    """Fix GET DIAGNOSTICS syntax issues (PostgreSQL → Snowflake)."""
    
    def __init__(self, config: PostProcessorConfig):
        super().__init__(config)
        self.fix_type = FixType.GET_DIAGNOSTICS
        
    def apply(self, content: str) -> FixResult:
        """Apply GET DIAGNOSTICS fixes."""
        original_content = content
        modified_content = content
        changes_made = 0
        applied_patterns = []
        
        try:
            # Apply each GET DIAGNOSTICS pattern
            for pattern, replacement in self.config.get_diagnostics_patterns.items():
                matches = re.findall(pattern, modified_content, re.IGNORECASE)
                if matches:
                    before_content = modified_content
                    modified_content = re.sub(pattern, replacement, modified_content, flags=re.IGNORECASE)
                    
                    if before_content != modified_content:
                        pattern_changes = len(matches)
                        changes_made += pattern_changes
                        applied_patterns.append(f"GET DIAGNOSTICS → SQLROWCOUNT ({pattern_changes} changes)")
                        logger.debug(f"Applied GET DIAGNOSTICS fix: {pattern} → {replacement}")
            
            return FixResult(
                success=True,
                fix_type=self.fix_type,
                changes_made=changes_made,
                original_content=original_content,
                modified_content=modified_content,
                applied_patterns=applied_patterns
            )
            
        except Exception as e:
            logger.error(f"GET DIAGNOSTICS fix failed: {e}")
            return FixResult(
                success=False,
                fix_type=self.fix_type,
                changes_made=0,
                error_message=str(e)
            )
    
    def can_apply(self, content: str) -> bool:
        """Check if GET DIAGNOSTICS fixes are needed."""
        for pattern in self.config.get_diagnostics_patterns.keys():
            if re.search(pattern, content, re.IGNORECASE):
                return True
        return False


class OverlapsOperatorFix(BaseFix):
    """Fix OVERLAPS operator syntax issues (Teradata → Snowflake)."""
    
    def __init__(self, config: PostProcessorConfig):
        super().__init__(config)
        self.fix_type = FixType.OVERLAPS_OPERATOR
        
    def apply(self, content: str) -> FixResult:
        """Apply OVERLAPS operator fixes."""
        original_content = content
        modified_content = content
        changes_made = 0
        applied_patterns = []
        
        try:
            # Apply each OVERLAPS pattern
            for pattern, replacement in self.config.overlaps_patterns.items():
                matches = re.findall(pattern, modified_content, re.IGNORECASE)
                if matches:
                    before_content = modified_content
                    modified_content = re.sub(pattern, replacement, modified_content, flags=re.IGNORECASE)
                    
                    if before_content != modified_content:
                        pattern_changes = len(matches)
                        changes_made += pattern_changes
                        applied_patterns.append(f"OVERLAPS → date range comparison ({pattern_changes} changes)")
                        logger.debug(f"Applied OVERLAPS fix: {pattern} → {replacement}")
            
            return FixResult(
                success=True,
                fix_type=self.fix_type,
                changes_made=changes_made,
                original_content=original_content,
                modified_content=modified_content,
                applied_patterns=applied_patterns
            )
            
        except Exception as e:
            logger.error(f"OVERLAPS operator fix failed: {e}")
            return FixResult(
                success=False,
                fix_type=self.fix_type,
                changes_made=0,
                error_message=str(e)
            )
    
    def can_apply(self, content: str) -> bool:
        """Check if OVERLAPS operator fixes are needed."""
        for pattern in self.config.overlaps_patterns.keys():
            if re.search(pattern, content, re.IGNORECASE):
                return True
        return False


class FileRenameLowercaseFix(BaseFix):
    """Fix for renaming files to lowercase."""
    
    def __init__(self, config: PostProcessorConfig):
        super().__init__(config)
        self.fix_type = FixType.FILE_RENAME_LOWERCASE
        self.target_file_path = None
        
    def set_target_file(self, file_path: str):
        """Set the target file for renaming."""
        self.target_file_path = Path(file_path)
        
    def apply(self, content: str) -> FixResult:
        """Apply file renaming (content is not modified, but file gets renamed)."""
        if not self.target_file_path:
            return FixResult(
                success=False,
                fix_type=self.fix_type,
                changes_made=0,
                error_message="No target file path set for renaming"
            )
        
        try:
            original_path = self.target_file_path
            # Create new path with lowercase name
            new_name = original_path.name.lower()
            new_path = original_path.parent / new_name
            
            # Check if renaming is needed
            if original_path.name == new_name:
                return FixResult(
                    success=True,
                    fix_type=self.fix_type,
                    changes_made=0,
                    original_content=content,
                    modified_content=content,
                    applied_patterns=["File already has lowercase name"]
                )
            
            # Perform the rename
            if original_path.exists():
                original_path.rename(new_path)
                logger.info(f"Renamed file: {original_path.name} → {new_name}")
                
                return FixResult(
                    success=True,
                    fix_type=self.fix_type,
                    changes_made=1,
                    original_content=content,
                    modified_content=content,
                    applied_patterns=[f"Renamed: {original_path.name} → {new_name}"]
                )
            else:
                return FixResult(
                    success=False,
                    fix_type=self.fix_type,
                    changes_made=0,
                    error_message=f"File does not exist: {original_path}"
                )
                
        except Exception as e:
            logger.error(f"File rename fix failed: {e}")
            return FixResult(
                success=False,
                fix_type=self.fix_type,
                changes_made=0,
                error_message=str(e)
            )
    
    def can_apply(self, content: str) -> bool:
        """Check if file renaming is needed."""
        if not self.target_file_path:
            return False
        
        # Check if filename has uppercase letters
        return self.target_file_path.name != self.target_file_path.name.lower()


class DataTypeConversionFix(BaseFix):
    """Fix data type conversions (VARCHAR/CHAR → STRING)."""
    
    def __init__(self, config: PostProcessorConfig):
        super().__init__(config)
        self.fix_type = FixType.DATA_TYPE_CONVERSION
        
    def apply(self, content: str) -> FixResult:
        """Apply data type conversion fixes."""
        original_content = content
        modified_content = content
        changes_made = 0
        applied_patterns = []
        
        try:
            # Apply each data type pattern
            for pattern, replacement in self.config.data_type_patterns.items():
                matches = re.findall(pattern, modified_content, re.IGNORECASE)
                if matches:
                    before_content = modified_content
                    modified_content = re.sub(pattern, replacement, modified_content, flags=re.IGNORECASE)
                    
                    if before_content != modified_content:
                        pattern_changes = len(matches)
                        changes_made += pattern_changes
                        applied_patterns.append(f"{pattern} → {replacement} ({pattern_changes} changes)")
                        logger.debug(f"Applied data type conversion: {pattern} → {replacement}")
            
            return FixResult(
                success=True,
                fix_type=self.fix_type,
                changes_made=changes_made,
                original_content=original_content,
                modified_content=modified_content,
                applied_patterns=applied_patterns
            )
            
        except Exception as e:
            logger.error(f"Data type conversion fix failed: {e}")
            return FixResult(
                success=False,
                fix_type=self.fix_type,
                changes_made=0,
                error_message=str(e)
            )
    
    def can_apply(self, content: str) -> bool:
        """Check if data type conversion fixes are needed."""
        for pattern in self.config.data_type_patterns.keys():
            if re.search(pattern, content, re.IGNORECASE):
                return True
        return False


class DefaultClauseRemovalFix(BaseFix):
    """Remove DEFAULT clauses for ICEBERG table compatibility."""
    
    def __init__(self, config: PostProcessorConfig):
        super().__init__(config)
        self.fix_type = FixType.DEFAULT_CLAUSE_REMOVAL
        
    def apply(self, content: str) -> FixResult:
        """Remove DEFAULT clauses."""
        original_content = content
        modified_content = content
        changes_made = 0
        applied_patterns = []
        
        try:
            # Only apply if this appears to be creating ICEBERG tables
            if not re.search(r'CREATE\s+(OR\s+REPLACE\s+)?ICEBERG\s+TABLE', modified_content, re.IGNORECASE):
                return FixResult(
                    success=True,
                    fix_type=self.fix_type,
                    changes_made=0,
                    original_content=original_content,
                    modified_content=modified_content,
                    applied_patterns=["Skipped: No ICEBERG tables detected"]
                )
            
            # Apply each default clause removal pattern
            for pattern in self.config.default_clause_patterns:
                matches = re.findall(pattern, modified_content, re.IGNORECASE)
                if matches:
                    before_content = modified_content
                    modified_content = re.sub(pattern, '', modified_content, flags=re.IGNORECASE)
                    
                    if before_content != modified_content:
                        pattern_changes = len(matches)
                        changes_made += pattern_changes
                        applied_patterns.append(f"Removed DEFAULT clauses ({pattern_changes} changes)")
                        logger.debug(f"Removed DEFAULT clauses using pattern: {pattern}")
            
            return FixResult(
                success=True,
                fix_type=self.fix_type,
                changes_made=changes_made,
                original_content=original_content,
                modified_content=modified_content,
                applied_patterns=applied_patterns
            )
            
        except Exception as e:
            logger.error(f"Default clause removal fix failed: {e}")
            return FixResult(
                success=False,
                fix_type=self.fix_type,
                changes_made=0,
                error_message=str(e)
            )
    
    def can_apply(self, content: str) -> bool:
        """Check if default clause removal is needed."""
        # Only apply to ICEBERG tables with DEFAULT clauses
        has_iceberg = re.search(r'CREATE\s+(OR\s+REPLACE\s+)?ICEBERG\s+TABLE', content, re.IGNORECASE)
        has_default = re.search(r'\bDEFAULT\b', content, re.IGNORECASE)
        return bool(has_iceberg and has_default)


class IcebergSyntaxFix(BaseFix):
    """Fix ICEBERG table syntax issues."""
    
    def __init__(self, config: PostProcessorConfig):
        super().__init__(config)
        self.fix_type = FixType.ICEBERG_SYNTAX
        
    def apply(self, content: str) -> FixResult:
        """Apply ICEBERG syntax fixes."""
        original_content = content
        modified_content = content
        changes_made = 0
        applied_patterns = []
        
        try:
            # Apply each ICEBERG table pattern
            for pattern, replacement in self.config.iceberg_table_patterns.items():
                matches = re.findall(pattern, modified_content, re.IGNORECASE)
                if matches:
                    before_content = modified_content
                    modified_content = re.sub(pattern, replacement, modified_content, flags=re.IGNORECASE)
                    
                    if before_content != modified_content:
                        pattern_changes = len(matches)
                        changes_made += pattern_changes
                        applied_patterns.append(f"{pattern} → {replacement} ({pattern_changes} changes)")
                        logger.debug(f"Applied ICEBERG syntax fix: {pattern} → {replacement}")
            
            return FixResult(
                success=True,
                fix_type=self.fix_type,
                changes_made=changes_made,
                original_content=original_content,
                modified_content=modified_content,
                applied_patterns=applied_patterns
            )
            
        except Exception as e:
            logger.error(f"ICEBERG syntax fix failed: {e}")
            return FixResult(
                success=False,
                fix_type=self.fix_type,
                changes_made=0,
                error_message=str(e)
            )
    
    def can_apply(self, content: str) -> bool:
        """Check if ICEBERG syntax fixes are needed."""
        for pattern in self.config.iceberg_table_patterns.keys():
            if re.search(pattern, content, re.IGNORECASE):
                return True
        return False


class CustomFix(BaseFix):
    """Apply custom user-defined fixes."""
    
    def __init__(self, config: PostProcessorConfig, custom_fixes: List[CustomFix] = None):
        super().__init__(config)
        self.fix_type = FixType.CUSTOM
        self.custom_fixes = custom_fixes or []
        
    def apply(self, content: str) -> FixResult:
        """Apply custom fixes."""
        original_content = content
        modified_content = content
        changes_made = 0
        applied_patterns = []
        
        try:
            # Apply each custom fix
            for custom_fix in self.custom_fixes:
                if not custom_fix.enabled:
                    continue
                    
                flags = 0 if custom_fix.case_sensitive else re.IGNORECASE
                
                if custom_fix.use_regex:
                    matches = re.findall(custom_fix.pattern, modified_content, flags)
                    if matches:
                        before_content = modified_content
                        modified_content = re.sub(
                            custom_fix.pattern, 
                            custom_fix.replacement, 
                            modified_content, 
                            flags=flags
                        )
                        
                        if before_content != modified_content:
                            pattern_changes = len(matches)
                            changes_made += pattern_changes
                            applied_patterns.append(
                                f"{custom_fix.name}: {custom_fix.pattern} → {custom_fix.replacement} "
                                f"({pattern_changes} changes)"
                            )
                            logger.debug(f"Applied custom fix '{custom_fix.name}': {pattern_changes} changes")
                else:
                    # Simple string replacement
                    search_str = custom_fix.pattern
                    if not custom_fix.case_sensitive:
                        count = modified_content.lower().count(search_str.lower())
                        if count > 0:
                            # Case-insensitive replacement (more complex for non-regex)
                            pattern = re.escape(search_str)
                            modified_content = re.sub(
                                pattern, 
                                custom_fix.replacement, 
                                modified_content, 
                                flags=re.IGNORECASE
                            )
                            changes_made += count
                            applied_patterns.append(
                                f"{custom_fix.name}: {search_str} → {custom_fix.replacement} ({count} changes)"
                            )
                    else:
                        count = modified_content.count(search_str)
                        if count > 0:
                            modified_content = modified_content.replace(search_str, custom_fix.replacement)
                            changes_made += count
                            applied_patterns.append(
                                f"{custom_fix.name}: {search_str} → {custom_fix.replacement} ({count} changes)"
                            )
            
            return FixResult(
                success=True,
                fix_type=self.fix_type,
                changes_made=changes_made,
                original_content=original_content,
                modified_content=modified_content,
                applied_patterns=applied_patterns
            )
            
        except Exception as e:
            logger.error(f"Custom fix failed: {e}")
            return FixResult(
                success=False,
                fix_type=self.fix_type,
                changes_made=0,
                error_message=str(e)
            )
    
    def can_apply(self, content: str) -> bool:
        """Check if any custom fixes can be applied."""
        return len([fix for fix in self.custom_fixes if fix.enabled]) > 0


# Fix factory function
def create_fix(fix_type: FixType, config: PostProcessorConfig, **kwargs) -> BaseFix:
    """Create a fix instance of the specified type."""
    fix_map = {
        FixType.DOLLAR_DELIMITER: DollarDelimiterFix,
        FixType.FILE_RENAME_LOWERCASE: FileRenameLowercaseFix,
        FixType.GET_DIAGNOSTICS: GetDiagnosticsFix,
        FixType.OVERLAPS_OPERATOR: OverlapsOperatorFix,
        FixType.DATA_TYPE_CONVERSION: DataTypeConversionFix,
        FixType.DEFAULT_CLAUSE_REMOVAL: DefaultClauseRemovalFix,
        FixType.ICEBERG_SYNTAX: IcebergSyntaxFix,
        FixType.CUSTOM: CustomFix
    }
    
    fix_class = fix_map.get(fix_type)
    if not fix_class:
        raise ValueError(f"Unknown fix type: {fix_type}")
    
    if fix_type == FixType.CUSTOM:
        return fix_class(config, **kwargs)
    else:
        return fix_class(config)
