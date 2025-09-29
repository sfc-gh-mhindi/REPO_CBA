"""
BTEQ Variable Substitution Service

Data preprocessing service for BTEQ variable substitution and transformation.
Handles %%VARIABLE%%, ${VARIABLE}, &VARIABLE patterns from configuration mappings.

Moved from substitution/substitution_service.py to services/preprocessing/substitution/service.py 
as part of clean service architecture refactoring.
"""

import re
import logging
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class SubstitutionMapping:
    """Represents a variable substitution mapping."""
    old_value: str
    new_value: str
    

class SubstitutionService:
    """Service for performing BTEQ variable substitutions."""
    
    def __init__(self, config_path: str = None):
        """
        Initialize the substitution service.
        
        Args:
            config_path: Path to config file with variable mappings
        """
        self.config_path = config_path or "config.cfg"
        self.substitution_mappings: List[SubstitutionMapping] = []
        self._load_config()
    
    def _load_config(self) -> None:
        """Load variable substitution mappings from config file."""
        try:
            config_file = Path(self.config_path)
            if not config_file.exists():
                logger.warning(f"Config file not found: {self.config_path}")
                return
                
            with open(config_file, 'r', encoding='utf-8') as f:
                for line_num, line in enumerate(f, 1):
                    line = line.strip()
                    if not line or line.startswith('#'):
                        continue
                        
                    if ',' in line:
                        parts = [part.strip() for part in line.split(',')]
                        if len(parts) >= 2:
                            old_val, new_val = parts[0], parts[1]
                            self.substitution_mappings.append(
                                SubstitutionMapping(old_val, new_val)
                            )
                            logger.debug(f"Loaded mapping: {old_val} -> {new_val}")
                        else:
                            logger.warning(f"Invalid mapping on line {line_num}: {line}")
                            
        except Exception as e:
            logger.error(f"Error loading config file {self.config_path}: {e}")
            raise
    
    def get_mappings(self) -> List[SubstitutionMapping]:
        """Get all loaded substitution mappings."""
        return self.substitution_mappings.copy()
    
    def substitute_variables(self, content: str) -> Tuple[str, int]:
        """
        Substitute BTEQ variables in content using loaded mappings.
        
        Args:
            content: Original BTEQ content
            
        Returns:
            Tuple of (substituted_content, substitution_count)
        """
        if not content:
            return content, 0
            
        substituted_content = content
        total_substitutions = 0
        
        for mapping in self.substitution_mappings:
            # Pattern for variable substitution - prioritized by specificity
            # More specific patterns first to avoid partial matches
            patterns = [
                (rf'%%{re.escape(mapping.old_value)}%%', mapping.new_value),     # %%VARIABLE%% format (BTEQ style)
                (rf'\$\{{{re.escape(mapping.old_value)}\}}', mapping.new_value), # ${VARIABLE} format
                (rf'&{re.escape(mapping.old_value)}\b', mapping.new_value),      # &VARIABLE format
                (rf':{re.escape(mapping.old_value)}\b', mapping.new_value),      # :VARIABLE format
                (rf'\b{re.escape(mapping.old_value)}\b', mapping.new_value),     # Direct word match (last)
            ]
            
            for pattern, replacement in patterns:
                old_content = substituted_content
                substituted_content = re.sub(
                    pattern, 
                    replacement, 
                    substituted_content, 
                    flags=re.IGNORECASE
                )
                
                # Count substitutions made for this pattern
                pattern_substitutions = len(re.findall(pattern, old_content, re.IGNORECASE))
                total_substitutions += pattern_substitutions
                
                if pattern_substitutions > 0:
                    logger.debug(f"Pattern '{pattern}' -> '{replacement}': {pattern_substitutions} substitutions")
        
        return substituted_content, total_substitutions
    
    def substitute_file(self, input_file: str, output_file: str) -> Dict[str, any]:
        """
        Substitute variables in a file and write to output.
        
        Args:
            input_file: Path to input file
            output_file: Path to output file
            
        Returns:
            Dictionary with substitution results
        """
        try:
            input_path = Path(input_file)
            output_path = Path(output_file)
            
            # Ensure output directory exists
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Read input file with encoding detection
            encodings_to_try = ['utf-8', 'windows-1252', 'iso-8859-1', 'utf-8-sig']
            original_content = None
            used_encoding = None
            
            for encoding in encodings_to_try:
                try:
                    with open(input_path, 'r', encoding=encoding) as f:
                        original_content = f.read()
                        used_encoding = encoding
                        break
                except UnicodeDecodeError:
                    continue
            
            if original_content is None:
                raise ValueError(f"Could not read file {input_path} with any supported encoding: {encodings_to_try}")
                
            logger.debug(f"Read file using {used_encoding} encoding")
            
            # Perform substitutions
            substituted_content, substitution_count = self.substitute_variables(original_content)
            
            # Write output file
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(substituted_content)
            
            result = {
                'input_file': str(input_path),
                'output_file': str(output_path),
                'original_size': len(original_content),
                'substituted_size': len(substituted_content),
                'substitution_count': substitution_count,
                'success': True
            }
            
            logger.info(f"Substituted {substitution_count} variables in {input_path.name}")
            return result
            
        except Exception as e:
            logger.error(f"Error substituting file {input_file}: {e}")
            return {
                'input_file': str(input_file),
                'output_file': str(output_file),
                'error': str(e),
                'success': False
            }
    
    def substitute_directory(self, input_dir: str, output_dir: str, 
                           file_pattern: str = "*.sql") -> List[Dict[str, any]]:
        """
        Substitute variables in all files matching pattern in a directory.
        
        Args:
            input_dir: Path to input directory
            output_dir: Path to output directory  
            file_pattern: File pattern to match (default: *.sql)
            
        Returns:
            List of substitution results for each file
        """
        results = []
        input_path = Path(input_dir)
        output_path = Path(output_dir)
        
        if not input_path.exists():
            logger.error(f"Input directory does not exist: {input_dir}")
            return results
        
        # Find all matching files
        matching_files = list(input_path.glob(file_pattern))
        logger.info(f"Found {len(matching_files)} files matching {file_pattern}")
        
        for input_file in matching_files:
            output_file = output_path / input_file.name
            result = self.substitute_file(str(input_file), str(output_file))
            results.append(result)
        
        return results
    
    def get_substitution_summary(self, results: List[Dict[str, any]]) -> Dict[str, any]:
        """
        Generate a summary of substitution results.
        
        Args:
            results: List of substitution results
            
        Returns:
            Summary dictionary
        """
        successful = [r for r in results if r.get('success', False)]
        failed = [r for r in results if not r.get('success', False)]
        
        total_substitutions = sum(r.get('substitution_count', 0) for r in successful)
        
        return {
            'total_files': len(results),
            'successful_files': len(successful),
            'failed_files': len(failed),
            'total_substitutions': total_substitutions,
            'average_substitutions_per_file': total_substitutions / len(successful) if successful else 0,
            'failed_file_names': [r.get('input_file', 'unknown') for r in failed]
        }
