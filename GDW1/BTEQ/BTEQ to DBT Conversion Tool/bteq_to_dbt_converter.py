#!/usr/bin/env python3
"""
BTEQ to DBT Framework Converter
===============================

This script automates the conversion of Teradata BTEQ scripts to a complete DBT framework
following established patterns and best practices.

Features:
- Analyzes BTEQ files and categorizes them by function
- Generates complete DBT project structure
- Creates configuration files (dbt_project.yml, profiles.yml, packages.yml)
- Converts BTEQ patterns to DBT patterns
- Generates comprehensive documentation
- Applies DCF framework integration
- Creates model templates and macros

Usage:
    python bteq_to_dbt_converter.py --source /path/to/bteq/files --target /path/to/dbt/project --project-name "MyProject"

Author: Cursor AI
Date: January 2025
"""

import os
import re
import json
import yaml
import argparse
import shutil
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
from datetime import datetime

@dataclass
class BTEQFile:
    """Represents a BTEQ file with metadata"""
    path: Path
    name: str
    category: str
    size_kb: float
    line_count: int
    content: str
    variables: List[str]
    complexity_score: int
    target_tables: List[str]  # Tables that this file inserts into
    has_insert_operation: bool = False

@dataclass
class ConversionConfig:
    """Configuration for the conversion process"""
    source_dir: Path
    target_dir: Path
    project_name: str
    snowflake_account: str = "your_account"
    database_prefix: str = "PSUND_MIGR"
    schema_prefix: str = "P_D_DCF_001_STD_0"
    config_file: Optional[Path] = None
    
    # Configuration loaded from YAML file
    model_categories: List[str] = None
    category_mapping: Dict[str, str] = None
    categorization_patterns: Dict[str, List[str]] = None
    additional_directories: List[str] = None
    
    def __post_init__(self):
        """Load configuration from YAML file if provided"""
        if self.config_file and self.config_file.exists():
            self._load_config_file()
        else:
            # Set default values if no config file
            self._set_default_config()
    
    def _load_config_file(self):
        """Load configuration from YAML file"""
        try:
            with open(self.config_file, 'r') as f:
                config_data = yaml.safe_load(f)
            
            self.model_categories = config_data.get('model_categories', [])
            self.category_mapping = config_data.get('category_mapping', {})
            self.categorization_patterns = config_data.get('categorization_patterns', {})
            self.additional_directories = config_data.get('additional_directories', [])
            
            print(f"✅ Loaded configuration from {self.config_file}")
        except Exception as e:
            print(f"⚠️  Error loading config file {self.config_file}: {e}")
            print("   Using default configuration")
            self._set_default_config()
    
    def _set_default_config(self):
        """Set default configuration values"""
        self.model_categories = [
            'account_balance',
            'derived_account_party', 
            'portfolio_technical',
            'process_control'
        ]
        
        self.category_mapping = {
            'account_balance': 'account_balance',
            'derived_account_party': 'derived_account_party',
            'portfolio_technical': 'portfolio_technical',
            'process_control': 'process_control',
            'data_loading': 'intermediate',
            'misc': 'intermediate',
            'configuration': None
        }
        
        self.categorization_patterns = {
            'account_balance': ['ACCT_BALN_BKDT'],
            'derived_account_party': ['DERV_ACCT_PATY'],
            'portfolio_technical': ['prtf_tech'],
            'process_control': ['sp_', '_SP_'],
            'data_loading': ['BTEQ_'],
            'configuration': ['login', '.snowct', 'storage']
        }

class BTEQAnalyzer:
    """Analyzes BTEQ files and categorizes them"""
    
    def __init__(self, source_dir: Path, config: ConversionConfig):
        self.source_dir = source_dir
        self.config = config
        self.files: List[BTEQFile] = []
    
    def analyze_files(self) -> List[BTEQFile]:
        """Analyze all BTEQ files in the source directory"""
        print(f"🔍 Analyzing BTEQ files in {self.source_dir}")
        
        for file_path in self.source_dir.glob("*"):
            if file_path.is_file() and not file_path.name.startswith('.'):
                bteq_file = self._analyze_file(file_path)
                if bteq_file:
                    self.files.append(bteq_file)
        
        print(f"✅ Analyzed {len(self.files)} BTEQ files")
        return self.files
    
    def _analyze_file(self, file_path: Path) -> Optional[BTEQFile]:
        """Analyze a single BTEQ file"""
        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
            
            # Extract variables from original content
            original_variables = re.findall(r'%%([A-Z_]+)%%', content)
            
            # Apply BTEQ conversions to see what variables remain after cleanup
            converted_content = self._apply_conversion_for_analysis(content)
            final_variables = re.findall(r'%%([A-Z_]+)%%', converted_content)
            
            # Filter out BTEQ-specific variables that are no longer needed
            filtered_variables = self._filter_bteq_variables(final_variables)
            
            # Categorize file
            category = self._categorize_file(file_path.name, content)
            
            # Calculate complexity score
            complexity = self._calculate_complexity(content)
            
            # Extract INSERT operations and target tables
            target_tables, has_insert = self._extract_insert_operations(content)
            
            return BTEQFile(
                path=file_path,
                name=file_path.name,
                category=category,
                size_kb=file_path.stat().st_size / 1024,
                line_count=len(content.splitlines()),
                content=content,
                variables=list(set(filtered_variables)),
                complexity_score=complexity,
                target_tables=target_tables,
                has_insert_operation=has_insert
            )
        except Exception as e:
            print(f"⚠️  Error analyzing {file_path.name}: {e}")
            return None
    
    def _categorize_file(self, filename: str, content: str) -> str:
        """Categorize a BTEQ file based on filename and content patterns"""
        filename_upper = filename.upper()
        
        # Use configurable categorization patterns
        for category, patterns in self.config.categorization_patterns.items():
            for pattern in patterns:
                if pattern in filename_upper:
                    return category
        
        # Content-based categorization
        if '.IMPORT' in content or '.EXPORT' in content:
            return 'data_loading'
        elif 'VOLATILE TABLE' in content.upper():
            return 'intermediate_processing'
        elif '.LOGON' in content or 'BTEQ' in content.upper():
            return 'configuration'
        
        return 'misc'
    
    def _extract_insert_operations(self, content: str) -> Tuple[List[str], bool]:
        """Extract INSERT operations and target table names"""
        target_tables = []
        has_insert = False
        
        # Pattern to match INSERT INTO statements
        # Handles both simple and schema-qualified table names with variables
        insert_patterns = [
            r'INSERT\s+INTO\s+(?:%%([A-Z_]+)%%)\.([A-Z_][A-Z0-9_]*)',  # INSERT INTO %%VAR%%.TABLE
            r'INSERT\s+INTO\s+([A-Z_][A-Z0-9_]*\.[A-Z_][A-Z0-9_]*)',   # INSERT INTO SCHEMA.TABLE
            r'INSERT\s+INTO\s+([A-Z_][A-Z0-9_]*)',                     # INSERT INTO TABLE
        ]
        
        for pattern in insert_patterns:
            matches = re.findall(pattern, content, re.IGNORECASE | re.MULTILINE)
            for match in matches:
                has_insert = True
                if isinstance(match, tuple):
                    if len(match) == 2 and match[0] and match[1]:  # Schema variable + table
                        table_name = match[1]  # Use just the table name
                    elif len(match) == 1:  # Single match
                        table_name = match[0].split('.')[-1] if '.' in match[0] else match[0]
                    else:
                        continue
                else:
                    table_name = match.split('.')[-1] if '.' in match else match
                
                # Clean up table name
                table_name = table_name.strip().upper()
                if table_name and table_name not in target_tables:
                    target_tables.append(table_name)
        
        return target_tables, has_insert
    
    def _apply_conversion_for_analysis(self, content: str) -> str:
        """Apply BTEQ conversions for analysis purposes only"""
        # Apply the same conversions as the main method but without full processing
        # This helps us see which variables remain after BTEQ command removal
        
        # Convert INSERT operations to SELECT
        content = re.sub(r'INSERT\s+INTO\s+[^(]+\([^)]+\)\s*(SELECT.*?)(?=;|\Z)', r'\1', content, flags=re.IGNORECASE | re.DOTALL)
        content = re.sub(r'INSERT\s+INTO\s+[^\s]+\s+(SELECT.*?)(?=;|\Z)', r'\1', content, flags=re.IGNORECASE | re.DOTALL)
        
        # Remove DELETE statements
        delete_patterns = [
            r'DELETE\s+FROM\s+[^;]+;',
            r'DELETE\s+[^;]+;',
            r'DELETE\s+{{[^}]+}}\.[^;]+;',
        ]
        for pattern in delete_patterns:
            content = re.sub(pattern, '', content, flags=re.IGNORECASE | re.MULTILINE)
        
        # Remove UPDATE statements
        update_patterns = [
            r'UPDATE\s+[^;]+SET[^;]+;',
            r'UPDATE\s+{{[^}]+}}\.[^\s]+\s+SET[^;]+;',
        ]
        for pattern in update_patterns:
            content = re.sub(pattern, '', content, flags=re.IGNORECASE | re.MULTILINE | re.DOTALL)
        
        # Remove LOCKING TABLE statements
        locking_patterns = [
            r'LOCKING\s+TABLE\s+[^;]+FOR\s+WRITE[^;]*;?',
            r'LOCKING\s+TABLE\s+{{[^}]+}}\.[^\s]+\s+FOR\s+WRITE[^;]*;?',
            r'LOCKING\s+TABLE\s+[^\n]+FOR\s+WRITE[^\n]*',
        ]
        for pattern in locking_patterns:
            content = re.sub(pattern, '', content, flags=re.IGNORECASE | re.MULTILINE)
        
        # Remove BTEQ commands (same patterns as main method)
        content = re.sub(r'\.RUN\s+FILE\s*=.*\n?', '', content, flags=re.IGNORECASE)
        content = re.sub(r'\.IF\s+(ERRORCODE|ERRORLEVEL|ACTIVITYCOUNT).*\n?', '', content, flags=re.IGNORECASE)
        content = re.sub(r'\.GOTO\s+\w+.*\n?', '', content, flags=re.IGNORECASE)
        content = re.sub(r'\.SET\s+\w+.*\n?', '', content, flags=re.IGNORECASE)
        content = re.sub(r'\.LOGON\s+.*\n?', '', content, flags=re.IGNORECASE)
        content = re.sub(r'\.OS\s+.*\n?', '', content, flags=re.IGNORECASE)
        content = re.sub(r'\.QUIT.*\n?', '', content, flags=re.IGNORECASE)
        content = re.sub(r'\.LOGOFF.*\n?', '', content, flags=re.IGNORECASE)
        content = re.sub(r'\.EXIT.*\n?', '', content, flags=re.IGNORECASE)
        content = re.sub(r'\.LABEL\s+\w+.*\n?', '', content, flags=re.IGNORECASE)
        content = re.sub(r'\.EXPORT\s+.*\n?', '', content, flags=re.IGNORECASE)
        content = re.sub(r'\.IMPORT\s+.*\n?', '', content, flags=re.IGNORECASE)
        content = re.sub(r'\.THEN\s*\n?', '', content, flags=re.IGNORECASE)
        content = re.sub(r'\.ELSE\s*\n?', '', content, flags=re.IGNORECASE)
        
        return content
    
    def _filter_bteq_variables(self, variables: List[str]) -> List[str]:
        """Filter out BTEQ-specific variables that are no longer needed after conversion"""
        # List of variables that are definitely only used in BTEQ commands that we remove
        bteq_only_variables = {
            'BTEQ_LOGON_SCRIPT',
            'BTEQ_LOGIN_SCRIPT', 
            'BTEQ_SCRIPT',
            'LOGON_SCRIPT',
            'LOGIN_SCRIPT'  # Note: Keeping ENV_C and STRM_C as they might be used in file paths
        }
        
        # Filter out BTEQ-only variables but keep others that might be used in SQL
        filtered = [var for var in variables if var not in bteq_only_variables]
        
        return filtered
    
    def _calculate_complexity(self, content: str) -> int:
        """Calculate complexity score based on BTEQ features"""
        score = 0
        
        # BTEQ commands
        score += len(re.findall(r'\.(RUN|IF|GOTO|SET|IMPORT|EXPORT|OS)', content)) * 2
        
        # SQL complexity
        score += len(re.findall(r'\b(SELECT|INSERT|UPDATE|DELETE|CREATE|DROP)\b', content, re.IGNORECASE))
        
        # Variables
        score += len(re.findall(r'%%[A-Z_]+%%', content))
        
        # Volatile tables
        score += len(re.findall(r'VOLATILE TABLE', content, re.IGNORECASE)) * 3
        
        # Stored procedure calls
        score += len(re.findall(r'CALL\s+\w+', content, re.IGNORECASE)) * 2
        
        return score

class DBTProjectGenerator:
    """Generates DBT project structure and files"""
    
    def __init__(self, config: ConversionConfig, files: List[BTEQFile]):
        self.config = config
        self.files = files
        self.categories = self._group_files_by_category()
        self.target_tables = self._collect_target_tables()
        self.runtime_config = self._load_runtime_config()
    
    def generate_project(self):
        """Generate complete DBT project - simplified one model per BTEQ file"""
        print(f"🏗️  Generating DBT project: {self.config.project_name}")
        
        # Create directory structure
        self._create_directory_structure()
        
        # Generate configuration files
        self._generate_dbt_project_yml()
        self._generate_profiles_yml()
        self._generate_packages_yml()
        
        # Generate macros
        self._generate_macros()
        
        # Generate models - one per BTEQ file
        self._generate_models()
        
        # Generate documentation
        self._generate_documentation()
        
        print(f"✅ DBT project generated in {self.config.target_dir}")
    
    def _group_files_by_category(self) -> Dict[str, List[BTEQFile]]:
        """Group files by category"""
        categories = {}
        for file in self.files:
            if file.category not in categories:
                categories[file.category] = []
            categories[file.category].append(file)
        return categories
    
    def _collect_target_tables(self) -> Dict[str, Dict]:
        """Collect all unique target tables from INSERT operations across all files"""
        target_tables = {}
        
        for file in self.files:
            if file.has_insert_operation:
                for table_name in file.target_tables:
                    if table_name not in target_tables:
                        target_tables[table_name] = {
                            'category': file.category,
                            'source_files': [],
                            'mapped_category': self.config.category_mapping.get(file.category, 'intermediate')
                        }
                    target_tables[table_name]['source_files'].append(file.name)
        
        return target_tables
    
    def _load_runtime_config(self) -> Dict[str, str]:
        """Load runtime configuration from RuntimeConfig.txt"""
        runtime_config = {}
        
        # Look for RuntimeConfig.txt in the same directory as the converter script
        runtime_config_path = self.config.target_dir.parent / 'RuntimeConfig.txt'
        
        if not runtime_config_path.exists():
            # Try current working directory
            runtime_config_path = Path.cwd() / 'RuntimeConfig.txt'
        
        if runtime_config_path.exists():
            try:
                with open(runtime_config_path, 'r') as f:
                    for line in f:
                        line = line.strip()
                        if ':' in line and not line.startswith('#'):
                            # Parse format like: DERV_ACCT_PATY.*.VTECH:PVTECH
                            if '.' in line and ':' in line:
                                left_part, value = line.rsplit(':', 1)
                                if '.' in left_part:
                                    # Extract variable name (last part before colon)
                                    variable_part = left_part.split('.')[-1]
                                    runtime_config[variable_part] = value
                
                print(f"✅ Loaded runtime configuration with {len(runtime_config)} variable mappings")
                
            except Exception as e:
                print(f"⚠️  Warning: Could not load RuntimeConfig.txt: {e}")
        else:
            print("ℹ️  RuntimeConfig.txt not found, using default variable values")
        
        return runtime_config
    
    def _create_directory_structure(self):
        """Create simplified DBT project directory structure"""
        # Simplified base directories - one model per BTEQ file approach
        base_dirs = [
            '',
            'models',
            'models/sources', 
            'macros',
            'macros/dcf',
            'analyses',
            'seeds',
            'tests'
        ]
        
        # Add model category directories (optional organization)
        for category in self.config.model_categories:
            base_dirs.append(f'models/{category}')
        
        # Add any additional directories from config
        if self.config.additional_directories:
            for additional_dir in self.config.additional_directories:
                if additional_dir not in base_dirs:
                    base_dirs.append(additional_dir)
        
        # Create all directories
        for dir_path in base_dirs:
            (self.config.target_dir / dir_path).mkdir(parents=True, exist_ok=True)
    
    def _generate_dbt_project_yml(self):
        """Generate dbt_project.yml configuration"""
        variables = set()
        for file in self.files:
            variables.update(file.variables)
        
        # Generate vars with runtime config values where available
        vars_dict = {}
        for var in sorted(variables):
            if var in self.runtime_config:
                vars_dict[var] = self.runtime_config[var]
                print(f"   📝 Using runtime value for {var}: {self.runtime_config[var]}")
            else:
                vars_dict[var] = f"REPLACE_WITH_{var}_VALUE"
        
        config = {
            'name': self.config.project_name.lower().replace(' ', '_'),
            'version': '1.0.0',
            'config-version': 2,
            'profile': f"{self.config.project_name.lower().replace(' ', '_')}_profile",
            
            'model-paths': ['models'],
            'analysis-paths': ['analyses'],
            'test-paths': ['tests'],
            'seed-paths': ['seeds'],
            'macro-paths': ['macros'],
            'snapshot-paths': ['snapshots'],
            
            'target-path': 'target',
            'clean-targets': ['target', 'dbt_packages'],
            
            'vars': vars_dict,
            
            'models': {
                self.config.project_name.lower().replace(' ', '_'): {
                    '+materialized': 'table',
                    '+docs': {'show': True},
                    '+pre_hook': "{{ register_process_instance(this.name) }}",
                    '+post_hook': "{{ update_process_status('SUCCESS') }}"
                }
            }
        }
        
        with open(self.config.target_dir / 'dbt_project.yml', 'w') as f:
            yaml.dump(config, f, default_flow_style=False, sort_keys=False)
    
    def _generate_profiles_yml(self):
        """Generate profiles.yml for Snowflake connection"""
        profile = {
            f"{self.config.project_name.lower().replace(' ', '_')}_profile": {
                'target': 'dev',
                'outputs': {
                    'dev': {
                        'type': 'snowflake',
                        'account': '{{ env_var("SNOWFLAKE_ACCOUNT") }}',#mh config
                        'user': '{{ env_var("SNOWFLAKE_USER") }}',#mh config
                        'password': '{{ env_var("SNOWFLAKE_PASSWORD") }}',#mh config
                        'role': '{{ env_var("SNOWFLAKE_ROLE") }}',#mh config
                        'database': f'{self.config.database_prefix}_DCF',#mh config
                        'warehouse': '{{ env_var("SNOWFLAKE_WAREHOUSE") }}',#mh config
                        'schema': self.config.schema_prefix,#mh config
                        'threads': 4,
                        'keepalives_idle': 240,
                        'search_path': f'{self.config.database_prefix}_DCF.{self.config.schema_prefix}'#mh config
                    },
                    'prod': {
                        'type': 'snowflake',
                        'account': '{{ env_var("SNOWFLAKE_ACCOUNT") }}',#mh config
                        'user': '{{ env_var("SNOWFLAKE_USER") }}',#mh config
                        'password': '{{ env_var("SNOWFLAKE_PASSWORD") }}',#mh config
                        'role': '{{ env_var("SNOWFLAKE_ROLE") }}',#mh config
                        'database': f'{self.config.database_prefix}_CLD',#mh config
                        'warehouse': '{{ env_var("SNOWFLAKE_WAREHOUSE") }}',#mh config
                        'schema': 'P_D_GDW_001_STD_0',#mh config
                        'threads': 8,
                        'keepalives_idle': 240
                    }
                }
            }
        }
        
        with open(self.config.target_dir / 'profiles.yml', 'w') as f:
            yaml.dump(profile, f, default_flow_style=False)
    
    def _generate_packages_yml(self):
        """Generate packages.yml for DBT dependencies"""
        packages = {
            'packages': [
                {'package': 'dbt-labs/dbt_utils', 'version': '1.1.1'},
                {'package': 'calogica/dbt_expectations', 'version': '0.10.1'}
            ]
        }
        
        with open(self.config.target_dir / 'packages.yml', 'w') as f:
            yaml.dump(packages, f, default_flow_style=False)
    
    def _generate_macros(self):
        """Generate DBT macros for BTEQ conversion patterns"""
        
        # DCF Logging macros
        dcf_logging = '''-- DCF Framework Integration Macros
-- Adapted for BTEQ to DBT conversion

{% macro register_process_instance(process_name, stream_code='DEFAULT') %}
  -- Register process instance in DCF framework
  INSERT INTO DCF_T_PRCS_INST (
    PRCS_NAME,
    STRM_C,
    STRT_TS,
    STAT_C,
    CREATED_TS
  ) VALUES (
    '{{ process_name }}',
    '{{ stream_code }}', 
    CURRENT_TIMESTAMP(),
    'RUNNING',
    CURRENT_TIMESTAMP()
  )
{% endmacro %}

{% macro update_process_status(status, message='') %}
  -- Update process status in DCF framework
  UPDATE DCF_T_PRCS_INST 
  SET 
    STAT_C = '{{ status }}',
    END_TS = CURRENT_TIMESTAMP(),
    UPDT_TS = CURRENT_TIMESTAMP()
    {% if message %}
    , MSG_T = '{{ message }}'
    {% endif %}
  WHERE PRCS_NAME = '{{ this.name }}'
    AND STRT_TS = (SELECT MAX(STRT_TS) FROM DCF_T_PRCS_INST WHERE PRCS_NAME = '{{ this.name }}')
{% endmacro %}

{% macro log_exec_msg(message_type, message_text) %}
  -- Log execution message
  INSERT INTO DCF_T_EXEC_LOG (
    PRCS_NAME,
    MSG_TYPE_C,
    MSG_T,
    CREATED_TS
  ) VALUES (
    '{{ this.name }}',
    '{{ message_type }}',
    '{{ message_text }}',
    CURRENT_TIMESTAMP()
  )
{% endmacro %}'''

        with open(self.config.target_dir / 'macros' / 'dcf' / 'logging.sql', 'w') as f:
            f.write(dcf_logging)
        
        # Common conversion utilities
        common_macros = '''-- Common BTEQ to DBT Conversion Utilities

{% macro bteq_var(var_name) %}
  -- Convert BTEQ variable reference to DBT variable
  {{ var(var_name) }}
{% endmacro %}

{% macro volatile_to_cte(table_name, sql) %}
  -- Convert BTEQ volatile table to CTE
  WITH {{ table_name }} AS (
    {{ sql }}
  )
{% endmacro %}

{% macro copy_from_stage(stage_path, target_table) %}
  -- Convert BTEQ .IMPORT to Snowflake COPY
  COPY INTO {{ target_table }}
  FROM @{{ stage_path }}
  FILE_FORMAT = (TYPE = 'CSV', FIELD_OPTIONALLY_ENCLOSED_BY = '"')
{% endmacro %}

{% macro copy_to_stage(source_table, stage_path) %}
  -- Convert BTEQ .EXPORT to Snowflake COPY
  COPY INTO @{{ stage_path }}
  FROM {{ source_table }}
  FILE_FORMAT = (TYPE = 'CSV', HEADER = TRUE)
{% endmacro %}

{% macro model_config() %}
  -- Standard configuration for models
  {{
    config(
      materialized='table',
      docs={'show': true},
      pre_hook="{{ register_process_instance(this.name) }}",
      post_hook="{{ update_process_status('SUCCESS') }}"
    )
  }}
{% endmacro %}'''

        with open(self.config.target_dir / 'macros' / 'dcf' / 'common.sql', 'w') as f:
            f.write(common_macros)
    
    def _generate_models(self):
        """Generate one DBT model per BTEQ file - simplified approach"""
        model_count = 0
        
        for category, files in self.categories.items():
            # Skip if category is configured to be null (e.g., configuration files)
            if self.config.category_mapping.get(category) is None:
                print(f"⏭️  Skipping category '{category}' (configured to skip)")
                continue
            
            # Determine target directory - use category for organization or flat structure
            if category in ['configuration']:
                continue  # Skip configuration files
            
            # Use category directory for organization, or models root for flat structure
            if len(self.config.model_categories) > 1:
                target_dir = self.config.target_dir / 'models' / category
            else:
                target_dir = self.config.target_dir / 'models'
            
            target_dir.mkdir(parents=True, exist_ok=True)
            
            for file in files:
                model_content = self._convert_bteq_to_dbt_model(file)
                
                # Generate clean model name from BTEQ filename
                model_name = file.name.replace('.sql', '').lower()
                
                model_file = target_dir / f"{model_name}.sql"
                with open(model_file, 'w') as f:
                    f.write(model_content)
                
                model_count += 1
                print(f"   ✅ Generated model: models/{category}/{model_name}.sql")
        
        print(f"🎯 Generated {model_count} models (one per BTEQ file)")
    
    # Mart generation methods removed - using simplified one-model-per-BTEQ approach
    def _generate_mart_models_DISABLED(self):
        """Generate mart models for target tables identified from INSERT operations"""
        if not self.target_tables:
            print("📋 No target tables identified for mart generation")
            return
        
        print(f"🏗️  Generating {len(self.target_tables)} mart models for target tables")
        
        for table_name, table_info in self.target_tables.items():
            category = table_info['mapped_category']
            if category is None:  # Skip if category is configured to skip
                continue
                
            self._generate_mart_model(table_name, table_info)
    
    def _generate_mart_model(self, table_name: str, table_info: Dict):
        """Generate a single mart model for a target table"""
        category = table_info['mapped_category']
        if category == 'intermediate':  # Skip intermediate categories for marts
            category = table_info['category']  # Use original category for mart placement
        
        # Create mart directory
        mart_dir = self.config.target_dir / 'models' / 'marts' / category
        mart_dir.mkdir(parents=True, exist_ok=True)
        
        # Generate mart model content
        mart_content = self._create_mart_model_content(table_name, table_info)
        
        # Write mart model file
        mart_filename = f"mart_{table_name.lower()}.sql"
        mart_file = mart_dir / mart_filename
        
        with open(mart_file, 'w') as f:
            f.write(mart_content)
        
        print(f"   ✅ Generated mart: models/marts/{category}/{mart_filename}")
    
    def _create_mart_model_content(self, table_name: str, table_info: Dict) -> str:
        """Create the content for a mart model"""
        source_files = table_info['source_files']
        category = table_info['category']
        
        # Find the corresponding intermediate models
        intermediate_refs = []
        for source_file in source_files:
            model_name = source_file.replace('.sql', '').lower()
            if not model_name.startswith('int_'):
                model_name = f"int_{model_name}"
            intermediate_refs.append(model_name)
        
        # Generate mart model header
        header = f'''-- =====================================================================
-- DBT Mart Model: {table_name}
-- Target Table: {table_name}
-- Category: {category}
-- Source Files: {', '.join(source_files)}
-- Intermediate Models: {', '.join(intermediate_refs)}
-- Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
-- =====================================================================

{{{{ marts_model_config(
    business_area='{category}',
    target_table='{table_name}'
) }}}}

'''
        
        # Generate SQL content
        if len(intermediate_refs) == 1:
            # Single source - simple reference
            sql_content = f'''-- Final {table_name} mart model
-- Materializes the target table from intermediate processing

SELECT 
    *,
    CURRENT_TIMESTAMP() AS MART_CREATED_TS,
    CURRENT_USER() AS MART_CREATED_BY
FROM {{{{ ref('{intermediate_refs[0]}') }}}}'''
        else:
            # Multiple sources - union or more complex logic
            sql_content = f'''-- Final {table_name} mart model  
-- Combines multiple intermediate models

WITH combined_data AS (
'''
            for i, ref in enumerate(intermediate_refs):
                if i > 0:
                    sql_content += '\n    UNION ALL\n'
                sql_content += f'''    SELECT 
        *,
        '{ref}' AS SOURCE_MODEL
    FROM {{{{ ref('{ref}') }}}}'''
            
            sql_content += f'''
)

SELECT 
    *,
    CURRENT_TIMESTAMP() AS MART_CREATED_TS,
    CURRENT_USER() AS MART_CREATED_BY
FROM combined_data'''

        return header + sql_content
    
    def _convert_bteq_to_dbt_model(self, file: BTEQFile) -> str:
        """Convert BTEQ file content to DBT model - simplified approach"""
        header = f'''-- =====================================================================
-- DBT Model: {file.name.replace('.sql', '')}
-- Converted from BTEQ: {file.name}
-- Category: {file.category}
-- Original Size: {file.size_kb:.1f}KB, {file.line_count} lines
-- Complexity Score: {file.complexity_score}
-- Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
-- =====================================================================

{{{{
    config(
        materialized='table',
        docs={{'show': true}}
    )
}}}}

'''
        
        # Convert BTEQ content
        converted_sql = self._apply_bteq_conversions(file.content)
        
        # Wrap in DBT structure
        if 'SELECT' in converted_sql.upper():
            # Query-based model
            model_content = header + converted_sql
        else:
            # Action-based model (INSERT/UPDATE/DELETE)
            model_content = header + f'''-- This model performs data modification operations
-- Execute via post-hook or separate run

{{{{ config(
    materialized='table',
    post_hook=[
        "{converted_sql.replace('"', '\\"')}"
    ]
) }}}}

-- Placeholder query for DBT execution
SELECT 1 as operation_complete
'''
        
        return model_content
    
    def _apply_bteq_conversions(self, content: str) -> str:
        """Apply BTEQ to Snowflake/DBT conversion patterns"""
        
        # Convert INSERT operations to SELECT for intermediate models
        content = self._convert_insert_to_select(content)
        
        # Remove DELETE statements (DBT handles via table materialization)
        content = self._remove_delete_statements(content)
        
        # Remove UPDATE statements (DBT models should be SELECT-only)
        content = self._remove_update_statements(content)
        
        # Remove LOCKING TABLE statements (Teradata-specific, not needed in Snowflake)
        content = self._remove_locking_statements(content)
        
        # Remove BTEQ commands (case-insensitive and comprehensive patterns)
        content = re.sub(r'\.RUN\s+FILE\s*=.*\n?', '', content, flags=re.IGNORECASE)
        content = re.sub(r'\.IF\s+(ERRORCODE|ERRORLEVEL|ACTIVITYCOUNT).*\n?', '', content, flags=re.IGNORECASE)
        content = re.sub(r'\.GOTO\s+\w+.*\n?', '', content, flags=re.IGNORECASE)
        content = re.sub(r'\.SET\s+\w+.*\n?', '', content, flags=re.IGNORECASE)
        content = re.sub(r'\.LOGON\s+.*\n?', '', content, flags=re.IGNORECASE)
        content = re.sub(r'\.OS\s+.*\n?', '', content, flags=re.IGNORECASE)
        content = re.sub(r'\.QUIT.*\n?', '', content, flags=re.IGNORECASE)
        content = re.sub(r'\.LOGOFF.*\n?', '', content, flags=re.IGNORECASE)
        content = re.sub(r'\.EXIT.*\n?', '', content, flags=re.IGNORECASE)
        content = re.sub(r'\.LABEL\s+\w+.*\n?', '', content, flags=re.IGNORECASE)
        content = re.sub(r'\.EXPORT\s+.*\n?', '', content, flags=re.IGNORECASE)
        content = re.sub(r'\.IMPORT\s+.*\n?', '', content, flags=re.IGNORECASE)
        content = re.sub(r'\.THEN\s*\n?', '', content, flags=re.IGNORECASE)
        content = re.sub(r'\.ELSE\s*\n?', '', content, flags=re.IGNORECASE)
        
        # Convert variable references
        content = re.sub(r'%%([A-Z_]+)%%', r'{{ bteq_var("\1") }}', content)
        
        # Convert volatile tables to CTEs (simple pattern)
        content = re.sub(
            r'CREATE MULTISET VOLATILE TABLE (\w+) AS\s*\((.*?)\)\s*WITH DATA PRIMARY INDEX.*?;',
            r'WITH \1 AS (\n    \2\n),',
            content,
            flags=re.DOTALL
        )
        
        # Convert Teradata-specific functions
        content = re.sub(r'\bADD_MONTHS\s*\((.*?),\s*(.*?)\)', r'DATEADD(MONTH, \2, \1)', content)
        content = re.sub(r'\bDATE\s*-\s*(\d+)', r'DATEADD(DAY, -\1, CURRENT_DATE())', content)
        content = re.sub(r"DATE'([^']+)'", r"'\1'::DATE", content)
        
        # Convert SEL to SELECT
        content = re.sub(r'\bSEL\b', 'SELECT', content)
        
        # Convert COLLECT STATISTICS (remove - not needed in Snowflake)
        content = re.sub(r'COLLECT STATISTICS.*?;', '-- Statistics collection not needed in Snowflake', content, flags=re.DOTALL)
        
        # Convert file operations to stage operations (commented out)
        content = re.sub(r'-- {{ copy_from_stage\((.*?)\) }}', r'-- Original IMPORT/EXPORT converted to stage operation: \1', content, flags=re.IGNORECASE)
        content = re.sub(r'-- {{ copy_to_stage\((.*?)\) }}', r'-- Original IMPORT/EXPORT converted to stage operation: \1', content, flags=re.IGNORECASE)
        
        return content
    
    def _convert_insert_to_select(self, content: str) -> str:
        """Convert INSERT INTO ... SELECT to just SELECT for DBT intermediate models"""
        # Pattern to match INSERT INTO ... SELECT statements
        # This handles multi-line INSERT statements with column lists
        
        # Pattern 1: INSERT INTO table (columns) SELECT ...
        pattern1 = r'INSERT\s+INTO\s+[^(]+\([^)]+\)\s*(SELECT.*?)(?=;|\Z)'
        matches = re.findall(pattern1, content, re.IGNORECASE | re.DOTALL)
        for match in matches:
            # Replace the INSERT portion with just the SELECT
            insert_block = re.search(r'INSERT\s+INTO\s+[^(]+\([^)]+\)\s*' + re.escape(match), content, re.IGNORECASE | re.DOTALL)
            if insert_block:
                replacement = f"-- Original INSERT converted to SELECT for DBT intermediate model\n{match.strip()}"
                content = content.replace(insert_block.group(0), replacement)
        
        # Pattern 2: INSERT INTO table SELECT ... (without column list)  
        pattern2 = r'INSERT\s+INTO\s+[^\s]+\s+(SELECT.*?)(?=;|\Z)'
        matches = re.findall(pattern2, content, re.IGNORECASE | re.DOTALL)
        for match in matches:
            insert_block = re.search(r'INSERT\s+INTO\s+[^\s]+\s+' + re.escape(match), content, re.IGNORECASE | re.DOTALL)
            if insert_block:
                replacement = f"-- Original INSERT converted to SELECT for DBT intermediate model\n{match.strip()}"
                content = content.replace(insert_block.group(0), replacement)
        
        return content
    
    def _remove_delete_statements(self, content: str) -> str:
        """Remove DELETE statements from BTEQ content for DBT compatibility"""
        # Pattern to match DELETE statements (various formats)
        delete_patterns = [
            r'DELETE\s+FROM\s+[^;]+;',                          # DELETE FROM table;
            r'DELETE\s+[^;]+;',                                 # DELETE table;
            r'DELETE\s+{{[^}]+}}\.[^;]+;',                      # DELETE {{ var("schema") }}.table;
        ]
        
        for pattern in delete_patterns:
            matches = re.findall(pattern, content, re.IGNORECASE | re.MULTILINE)
            for match in matches:
                # Replace DELETE statement with comment explaining the change
                replacement = f'-- Original DELETE removed: {match.strip()}\n-- DBT handles table replacement via materialization strategy'
                content = content.replace(match, replacement)
        
        return content
    
    def _remove_update_statements(self, content: str) -> str:
        """Remove UPDATE statements from BTEQ content for DBT compatibility"""
        # Pattern to match UPDATE statements (various formats)
        # More precise patterns to handle multi-line statements
        update_patterns = [
            r'UPDATE\s+[^;]+SET[^;]+;',                         # UPDATE table SET ... WHERE ...;
            r'UPDATE\s+{{[^}]+}}\.[^\s]+\s+SET[^;]+;',          # UPDATE {{ var("schema") }}.table SET ...;
        ]
        
        for pattern in update_patterns:
            content = re.sub(pattern, 
                           lambda m: f'-- Original UPDATE removed: {m.group(0).strip()}\n-- DBT models should be SELECT-only; UPDATE logic should be handled in post-hooks or separate processes\n',
                           content, 
                           flags=re.IGNORECASE | re.MULTILINE | re.DOTALL)
        
        return content
    
    def _remove_locking_statements(self, content: str) -> str:
        """Remove LOCKING TABLE statements from BTEQ content (Teradata-specific)"""
        # Pattern to match LOCKING TABLE statements - more precise
        locking_patterns = [
            r'LOCKING\s+TABLE\s+[^;]+FOR\s+WRITE[^;]*;?',       # LOCKING TABLE schema.table FOR WRITE;
            r'LOCKING\s+TABLE\s+{{[^}]+}}\.[^\s]+\s+FOR\s+WRITE[^;]*;?',  # LOCKING TABLE {{ var("schema") }}.table FOR WRITE;
            r'LOCKING\s+TABLE\s+[^\n]+FOR\s+WRITE[^\n]*',       # LOCKING TABLE without semicolon
        ]
        
        for pattern in locking_patterns:
            content = re.sub(pattern, 
                           lambda m: f'-- Original LOCKING removed: {m.group(0).strip()}\n-- Snowflake handles concurrency automatically; explicit locking not needed\n',
                           content, 
                           flags=re.IGNORECASE | re.MULTILINE)
        
        return content
    
    def _generate_documentation(self):
        """Generate comprehensive project documentation"""
        
        # Generate README.md
        readme_content = f'''# {self.config.project_name} - BTEQ to DBT Migration

## Overview

This DBT project was automatically generated from {len(self.files)} BTEQ files using the BTEQ to DBT converter.
The project uses a simplified approach with **one DBT model per BTEQ file** and includes DCF framework integration for enterprise-grade logging and process control.

## Project Statistics

- **Total Files Converted**: {len(self.files)}
- **Total Models Generated**: {len(self.files)} (one per BTEQ file)
- **Categories**: {len(self.categories)}
- **Total Lines of Code**: {sum(f.line_count for f in self.files):,}
- **Average Complexity Score**: {sum(f.complexity_score for f in self.files) / len(self.files):.1f}

## File Categories

{self._generate_category_summary()}

## Conversion Approach

This project uses a **simplified one-model-per-BTEQ-file approach**:
- Each BTEQ file becomes exactly one DBT model
- No intermediate/marts layer complexity  
- Direct conversion with BTEQ-to-Snowflake transformations applied
- Models are organized by category for better navigation

## Setup Instructions

### 1. Environment Setup
```bash
# Install DBT
pip install dbt-snowflake

# Navigate to project
cd {self.config.target_dir}

# Install dependencies
dbt deps
```

### 2. Configuration
Set environment variables:
```bash
export SNOWFLAKE_ACCOUNT=your_account
export SNOWFLAKE_USER=your_user
export SNOWFLAKE_PASSWORD=your_password
export SNOWFLAKE_ROLE=your_role
export SNOWFLAKE_WAREHOUSE=your_warehouse
```

### 3. Variable Configuration
Update variables in `dbt_project.yml`:
```yaml
vars:
{self._generate_variable_config()}
```

### 4. Initial Testing
```bash
# Test connection
dbt debug

# Compile models
dbt compile

# Run models
dbt run

# Run tests
dbt test
```

## Model Structure

{self._generate_model_structure_tree()}

## Generated Files

{self._generate_file_list()}

---

**Generated**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}  
**Converter Version**: 1.0  
**Total Conversion Time**: Automated
'''

        with open(self.config.target_dir / 'README.md', 'w') as f:
            f.write(readme_content)
        
        # Generate conversion summary
        self._generate_conversion_summary()
    
    def _generate_category_summary(self) -> str:
        """Generate summary of file categories"""
        summary = []
        for category, files in self.categories.items():
            total_lines = sum(f.line_count for f in files)
            avg_complexity = sum(f.complexity_score for f in files) / len(files)
            summary.append(f"- **{category.replace('_', ' ').title()}**: {len(files)} files, {total_lines:,} lines, complexity {avg_complexity:.1f}")
        return '\n'.join(summary)
    
    def _generate_target_table_summary(self) -> str:
        """Generate summary of target tables and mart models"""
        if not self.target_tables:
            return "No target tables identified from INSERT operations."
        
        summary = ["The following target tables were identified from INSERT operations and corresponding mart models were generated:\n"]
        
        for table_name, table_info in self.target_tables.items():
            category = table_info['category']
            source_files = table_info['source_files']
            mart_category = table_info['mapped_category']
            if mart_category == 'intermediate':
                mart_category = category
            
            summary.append(f"### {table_name}")
            summary.append(f"- **Category**: {category.replace('_', ' ').title()}")
            summary.append(f"- **Mart Location**: `models/marts/{mart_category}/mart_{table_name.lower()}.sql`")
            summary.append(f"- **Source Files**: {', '.join(source_files)}")
            summary.append("")
        
        return '\n'.join(summary)
    
    def _generate_variable_config(self) -> str:
        """Generate variable configuration example"""
        variables = set()
        for file in self.files:
            variables.update(file.variables)
        
        config_lines = []
        for var in sorted(variables):
            config_lines.append(f"  {var}: \"YOUR_{var}_VALUE\"")
        
        return '\n'.join(config_lines) if config_lines else "  # No variables found"
    
    def _generate_file_list(self) -> str:
        """Generate list of converted files - simplified approach"""
        file_list = []
        for category, files in self.categories.items():
            if category == 'configuration':
                continue
            file_list.append(f"\n### {category.replace('_', ' ').title()}")
            for file in sorted(files, key=lambda x: x.name):
                model_name = file.name.replace('.sql', '').lower()
                file_list.append(f"- `{model_name}.sql` ← `{file.name}` ({file.size_kb:.1f}KB, {file.line_count} lines)")
        
        return '\n'.join(file_list)
    
    def _generate_conversion_summary(self):
        """Generate detailed conversion summary"""
        summary_content = f'''# BTEQ to DBT Conversion Summary

## Conversion Statistics

**Generated**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}  
**Source Directory**: {self.config.source_dir}  
**Target Directory**: {self.config.target_dir}  
**Project Name**: {self.config.project_name}

## Files Processed

| File | Category | Size (KB) | Lines | Complexity | Variables |
|------|----------|-----------|-------|------------|-----------|
{self._generate_file_table()}

## Conversion Patterns Applied

### 1. BTEQ Commands Removed
- `.RUN FILE`, `.IF ERRORCODE`, `.GOTO`, `.SET` commands
- Replaced with DBT configuration and hooks

### 2. Variable References Converted
- `%%VARIABLE%%` → `{{{{ bteq_var('VARIABLE') }}}}`

### 3. Volatile Tables → CTEs
- `CREATE MULTISET VOLATILE TABLE` → `WITH table_name AS (...)`

### 4. File Operations → Stage Operations  
- `.IMPORT/.EXPORT` → Snowflake stage operations (commented)

### 5. Teradata Functions → Snowflake Functions
- `ADD_MONTHS()` → `DATEADD(MONTH, ...)`
- `DATE'YYYY-MM-DD'` → `'YYYY-MM-DD'::DATE`

## Next Steps

1. **Review Generated Models**: Validate conversion accuracy
2. **Configure Variables**: Update `dbt_project.yml` with correct values
3. **Test Models**: Run `dbt run` and `dbt test`
4. **Customize Logic**: Adjust business logic as needed
5. **Deploy**: Move to production environment

## Support

- Review generated README.md for setup instructions
- Check individual model files for conversion notes
- Refer to DBT documentation for advanced configurations
'''

        with open(self.config.target_dir / 'CONVERSION_SUMMARY.md', 'w') as f:
            f.write(summary_content)
    
    def _generate_file_table(self) -> str:
        """Generate file statistics table"""
        rows = []
        for file in sorted(self.files, key=lambda x: x.category + x.name):
            variables_str = ', '.join(file.variables[:3])
            if len(file.variables) > 3:
                variables_str += f" (+{len(file.variables) - 3} more)"
            rows.append(f"| {file.name} | {file.category} | {file.size_kb:.1f} | {file.line_count} | {file.complexity_score} | {variables_str} |")
        return '\n'.join(rows)

    def _generate_model_structure_tree(self) -> str:
        """Generates a string representation of the simplified model structure tree."""
        tree_str = "```\n"
        tree_str += "models/\n"
        tree_str += "├── sources/\n"
        for i, cat in enumerate(self.config.model_categories):
            prefix = "├──" if i < len(self.config.model_categories) - 1 else "└──"
            tree_str += f"{prefix} {cat}/\n"
        tree_str += "```\n\n"
        tree_str += "**Structure**: One DBT model per original BTEQ file, organized by category.\n"
        return tree_str

    def _generate_category_list(self, category_type: str) -> str:
        """Generates a string representation of a category list for the README."""
        if category_type == 'intermediate':
            return '\n'.join([f"│   ├── {cat.replace('_', ' ').title()}" for cat in self.config.model_categories])
        elif category_type == 'marts':
            return '\n'.join([f"│   └── {cat.replace('_', ' ').title()}" for cat in self.config.model_categories])
        return ""

def main():
    """Main conversion function"""
    parser = argparse.ArgumentParser(
        description='Convert BTEQ files to DBT framework',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  python bteq_to_dbt_converter.py --source ./Original_Files --target ./My_DBT_Project --project-name "GDW1 Migration"
  python bteq_to_dbt_converter.py -s /path/to/bteq -t /path/to/dbt -n "Data Warehouse" --config converter_config.yaml
  python bteq_to_dbt_converter.py -s ./bteq -t ./dbt -n "Migration" -c config.yaml --snowflake-account myaccount
        '''
    )
    
    parser.add_argument('-s', '--source', required=True, help='Source directory containing BTEQ files')
    parser.add_argument('-t', '--target', required=True, help='Target directory for DBT project')
    parser.add_argument('-n', '--project-name', required=True, help='Name of the DBT project')
    parser.add_argument('-c', '--config', help='Path to YAML configuration file (optional)')
    parser.add_argument('--snowflake-account', default='your_account', help='Snowflake account identifier')
    parser.add_argument('--database-prefix', default='PSUND_MIGR', help='Database name prefix')
    parser.add_argument('--schema-prefix', default='P_D_DCF_001_STD_0', help='Schema name prefix')
    
    args = parser.parse_args()
    
    # Validate inputs
    source_dir = Path(args.source)
    if not source_dir.exists():
        print(f"❌ Source directory does not exist: {source_dir}")
        return 1
    
    target_dir = Path(args.target)
    if target_dir.exists():
        response = input(f"Target directory exists: {target_dir}. Continue? (y/N): ")
        if response.lower() != 'y':
            return 1
    
    # Create configuration
    config_file = Path(args.config) if args.config else None
    
    config = ConversionConfig(
        source_dir=source_dir,
        target_dir=target_dir,
        project_name=args.project_name,
        snowflake_account=args.snowflake_account,
        database_prefix=args.database_prefix,
        schema_prefix=args.schema_prefix,
        config_file=config_file
    )
    
    print(f"🚀 Starting BTEQ to DBT conversion")
    print(f"   Source: {config.source_dir}")
    print(f"   Target: {config.target_dir}")
    print(f"   Project: {config.project_name}")
    
    try:
        # Analyze BTEQ files
        analyzer = BTEQAnalyzer(config.source_dir, config)
        files = analyzer.analyze_files()
        
        if not files:
            print("❌ No BTEQ files found to convert")
            return 1
        
        # Generate DBT project
        generator = DBTProjectGenerator(config, files)
        generator.generate_project()
        
        print(f"\n🎉 Conversion completed successfully!")
        print(f"   Generated {len(files)} model files")
        print(f"   Project ready at: {config.target_dir}")
        print(f"\nNext steps:")
        print(f"   1. cd {config.target_dir}")
        print(f"   2. Review and update dbt_project.yml variables")
        print(f"   3. Run 'dbt deps' to install packages")
        print(f"   4. Run 'dbt debug' to test connection")
        print(f"   5. Run 'dbt run' to execute models")
        
        return 0
        
    except Exception as e:
        print(f"❌ Conversion failed: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == '__main__':
    exit(main()) 