# DMVA V2 In-Flight Data Masking Implementation Plan

## 🎯 **Project Overview**

### **Objective**
Implement in-flight data masking capabilities for DMVA V2 that enables secure data migration from Teradata sources with the following requirements:
- Allow users to specify which columns need to be masked
- Provide dedicated masking functions for strings, numbers, and dates
- Ensure data is never stored unmasked in any stage or temporary location
- Apply masking exclusively to Teradata source systems

### **Key Benefits**
- **Security**: Sensitive data is masked at the source before any storage or transmission
- **Compliance**: Meets data privacy regulations by preventing exposure of PII/PHI
- **Flexibility**: Column-level granular control over masking policies
- **Performance**: In-flight masking with minimal performance overhead
- **Integrity**: Maintains referential integrity through deterministic masking options

---

## 📋 **Detailed Implementation Plan**

### **Phase 1: Database Schema Extensions**

#### 1.1 Extend `dmva_column_info` Table
**File**: `snowflake/03_Tables/dmva_column_info.sql`

**Purpose**: Add masking configuration columns to existing column metadata table

```sql
-- Add new columns for masking configuration
ALTER TABLE dmva_column_info ADD COLUMN (
    masking_enabled BOOLEAN DEFAULT FALSE,
    masking_function_type VARCHAR(50), -- 'STRING', 'NUMBER', 'DATE'
    masking_parameters VARIANT,        -- JSON config for masking parameters
    masking_seed VARCHAR(100)          -- Optional seed for deterministic masking
);
```

**Impact**: Extends existing column metadata with masking configuration without breaking existing functionality.

#### 1.2 Create Masking Configuration Table
**New File**: `snowflake/03_Tables/dmva_masking_config.sql`

**Purpose**: Centralized configuration for pattern-based masking rule assignment

```sql
CREATE TABLE dmva_masking_config (
    config_id NUMBER AUTOINCREMENT,
    system_name VARCHAR NOT NULL,
    database_pattern VARCHAR,
    schema_pattern VARCHAR,
    table_pattern VARCHAR,
    column_pattern VARCHAR,
    data_type_pattern VARCHAR,
    masking_function_type VARCHAR(50) NOT NULL,
    masking_parameters VARIANT,
    priority NUMBER DEFAULT 100,
    active BOOLEAN DEFAULT TRUE,
    created_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_ts TIMESTAMP_NTZ
);
```

**Use Cases**:
- Bulk configuration of masking policies
- Pattern-based rule application (e.g., all columns named '*SSN*' get partial masking)
- Priority-based rule resolution for overlapping patterns

---

### **Phase 2: Core Masking Functions**

#### 2.1 Create Masking Function Library
**New File**: `snowflake/05_Functions/dmva_masking_functions.sql`

**Purpose**: Provide three core masking functions for different data types

##### String Masking Function
```sql
CREATE OR REPLACE FUNCTION dmva_mask_string(
    input_value VARCHAR,
    mask_type VARCHAR,
    parameters VARIANT
)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
HANDLER = 'mask_string'
AS $$
def mask_string(input_value, mask_type, parameters):
    import hashlib
    import random
    import re
    
    if input_value is None:
        return None
        
    params = parameters if parameters else {}
    
    if mask_type == 'HASH':
        seed = params.get('seed', 'default_seed')
        return hashlib.sha256(f"{input_value}{seed}".encode()).hexdigest()[:len(input_value)]
    elif mask_type == 'PARTIAL':
        show_chars = params.get('show_chars', 4)
        mask_char = params.get('mask_char', 'X')
        if len(input_value) <= show_chars:
            return mask_char * len(input_value)
        return mask_char * (len(input_value) - show_chars) + input_value[-show_chars:]
    elif mask_type == 'RANDOM':
        chars = params.get('charset', 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789')
        return ''.join(random.choice(chars) for _ in range(len(input_value)))
    elif mask_type == 'REDACT':
        return '[REDACTED]'
    else:
        return input_value
$$;
```

**Supported String Masking Types**:
- `HASH`: Deterministic hash-based masking (preserves joins)
- `PARTIAL`: Show last N characters, mask the rest
- `RANDOM`: Replace with random characters
- `REDACT`: Replace with fixed redaction text

##### Number Masking Function
```sql
CREATE OR REPLACE FUNCTION dmva_mask_number(
    input_value NUMBER,
    mask_type VARCHAR,
    parameters VARIANT
)
RETURNS NUMBER
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
HANDLER = 'mask_number'
AS $$
def mask_number(input_value, mask_type, parameters):
    import random
    import hashlib
    
    if input_value is None:
        return None
        
    params = parameters if parameters else {}
    
    if mask_type == 'RANGE':
        min_val = params.get('min_value', 0)
        max_val = params.get('max_value', 100000)
        return random.randint(min_val, max_val)
    elif mask_type == 'NOISE':
        noise_percent = params.get('noise_percent', 10) / 100
        noise = random.uniform(-noise_percent, noise_percent)
        return int(input_value * (1 + noise))
    elif mask_type == 'BUCKET':
        buckets = params.get('buckets', [1000, 5000, 10000])
        for bucket in sorted(buckets):
            if input_value <= bucket:
                return bucket
        return buckets[-1]
    elif mask_type == 'HASH':
        seed = params.get('seed', 'default_seed')
        hash_val = int(hashlib.sha256(f"{input_value}{seed}".encode()).hexdigest()[:8], 16)
        return hash_val % 1000000  # Keep reasonable range
    else:
        return input_value
$$;
```

**Supported Number Masking Types**:
- `RANGE`: Replace with random number in specified range
- `NOISE`: Add percentage-based noise to original value
- `BUCKET`: Replace with bucket/range values
- `HASH`: Deterministic hash-based numeric replacement

##### Date Masking Function
```sql
CREATE OR REPLACE FUNCTION dmva_mask_date(
    input_value DATE,
    mask_type VARCHAR,
    parameters VARIANT
)
RETURNS DATE
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
HANDLER = 'mask_date'
AS $$
def mask_date(input_value, mask_type, parameters):
    import random
    from datetime import datetime, timedelta
    
    if input_value is None:
        return None
        
    params = parameters if parameters else {}
    
    if mask_type == 'RANGE':
        start_date = datetime.strptime(params.get('start_date', '2020-01-01'), '%Y-%m-%d')
        end_date = datetime.strptime(params.get('end_date', '2023-12-31'), '%Y-%m-%d')
        random_days = random.randint(0, (end_date - start_date).days)
        return (start_date + timedelta(days=random_days)).date()
    elif mask_type == 'SHIFT':
        shift_days = params.get('shift_days', random.randint(-365, 365))
        return input_value + timedelta(days=shift_days)
    elif mask_type == 'YEAR_ONLY':
        return datetime(input_value.year, 1, 1).date()
    elif mask_type == 'MONTH_ONLY':
        return datetime(input_value.year, input_value.month, 1).date()
    else:
        return input_value
$$;
```

**Supported Date Masking Types**:
- `RANGE`: Replace with random date in specified range
- `SHIFT`: Shift date by random or fixed number of days
- `YEAR_ONLY`: Truncate to year (January 1st)
- `MONTH_ONLY`: Truncate to month (1st day of month)

---

### **Phase 3: Masking Configuration Management**

#### 3.1 Create Masking Configuration Procedures
**New File**: `snowflake/08_All_Other_Procedures/dmva_configure_masking.sql`

**Purpose**: Provide user-friendly interface for configuring masking policies

```sql
CREATE OR REPLACE PROCEDURE dmva_configure_masking(
    system_name VARCHAR,
    table_patterns VARIANT,
    column_config VARIANT
)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'configure_masking'
AS $$
def configure_masking(session, system_name, table_patterns, column_config):
    """
    Configure masking for specific tables and columns
    
    Args:
        system_name: Target Teradata system name
        table_patterns: {'database': {'schema': ['table1', 'table2']}}
        column_config: {'column_name': {'type': 'STRING', 'method': 'HASH', 'params': {...}}}
    """
    
    # Validate system is Teradata
    system_check = session.sql(f"""
        SELECT system_type FROM dmva_systems 
        WHERE system_name = '{system_name}' AND is_source = TRUE
    """).collect()
    
    if not system_check or system_check[0]['SYSTEM_TYPE'].upper() != 'TERADATA':
        return f"Error: Masking only supported for Teradata source systems. System '{system_name}' is not a valid Teradata source."
    
    # Implementation logic here
    # Update dmva_column_info with masking configuration
    # Validate configuration parameters
    # Return success/error message
    
    return f"Masking configuration applied successfully for system '{system_name}'"
$$;

CREATE OR REPLACE PROCEDURE dmva_apply_masking_rules()
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'apply_masking_rules'
AS $$
def apply_masking_rules(session):
    """
    Apply pattern-based masking rules from dmva_masking_config
    to dmva_column_info for matching columns
    """
    
    # Get all active masking rules
    rules = session.sql("""
        SELECT * FROM dmva_masking_config 
        WHERE active = TRUE 
        ORDER BY priority ASC
    """).collect()
    
    # Apply rules to matching columns
    # Handle priority-based rule resolution
    # Return summary of applied rules
    
    return "Masking rules applied successfully"
$$;
```

**Usage Examples**:
```sql
-- Configure specific column masking
CALL dmva_configure_masking(
    'TERADATA_PROD',
    {'HR_DB': {'EMPLOYEE_SCHEMA': ['EMPLOYEES', 'CONTRACTORS']}},
    {
        'SSN': {'type': 'STRING', 'method': 'PARTIAL', 'params': {'show_chars': 4}},
        'SALARY': {'type': 'NUMBER', 'method': 'NOISE', 'params': {'noise_percent': 15}},
        'BIRTH_DATE': {'type': 'DATE', 'method': 'SHIFT', 'params': {'shift_days': 30}}
    }
);
```

---

### **Phase 4: Query Generation Enhancement**

#### 4.1 Modify Unload Task Generation
**File**: `snowflake/08_All_Other_Procedures/dmva_get_unload_tasks.sql`

**Purpose**: Integrate masking logic into SQL query generation for Teradata unload tasks

**Key Changes**:
1. Add masking validation check for Teradata systems
2. Modify SQL generation to include masking function calls
3. Ensure masking is applied before any data export

```python
# Add to the SQL generation logic (around line 150-200)
def apply_masking_to_query(session, system_type, query_sql, object_id):
    """Apply masking transformations to Teradata queries"""
    
    if system_type.upper() != 'TERADATA':
        return query_sql
    
    # Get masking configuration for columns
    masking_info = session.sql(f"""
        SELECT 
            column_name, 
            masking_function_type, 
            masking_parameters,
            masking_seed,
            data_type
        FROM dmva_column_info 
        WHERE object_id = {object_id} 
        AND masking_enabled = TRUE
    """).collect()
    
    if not masking_info:
        return query_sql
    
    # Transform SELECT clause to include masking functions
    # Replace column references with masked versions
    # Generate Teradata-compatible masking SQL
    
    return modified_query
```

#### 4.2 Create Teradata-Specific Masking SQL Generator
**New File**: `local/connections/teradata_masking.py`

**Purpose**: Generate Teradata-native SQL expressions for masking functions

```python
class TeradataMaskingGenerator:
    """Generates Teradata-specific masking SQL expressions"""
    
    def __init__(self):
        self.function_map = {
            'STRING': {
                'HASH': self._generate_string_hash,
                'PARTIAL': self._generate_string_partial,
                'RANDOM': self._generate_string_random,
                'REDACT': self._generate_string_redact
            },
            'NUMBER': {
                'RANGE': self._generate_number_range,
                'NOISE': self._generate_number_noise,
                'BUCKET': self._generate_number_bucket,
                'HASH': self._generate_number_hash
            },
            'DATE': {
                'RANGE': self._generate_date_range,
                'SHIFT': self._generate_date_shift,
                'YEAR_ONLY': self._generate_date_year,
                'MONTH_ONLY': self._generate_date_month
            }
        }
    
    def generate_masking_expression(self, column_name, data_type, mask_type, parameters):
        """Generate Teradata-specific masking SQL expression"""
        
        data_type_category = self._categorize_data_type(data_type)
        
        if data_type_category not in self.function_map:
            return column_name  # No masking for unsupported types
        
        if mask_type not in self.function_map[data_type_category]:
            return column_name  # No masking for unsupported methods
        
        generator_func = self.function_map[data_type_category][mask_type]
        return generator_func(column_name, parameters)
    
    def _generate_string_hash(self, column_name, params):
        """Generate Teradata HASH expression for strings"""
        seed = params.get('seed', 'default_seed')
        return f"SUBSTR(HASHROW({column_name} || '{seed}'), 1, CHARACTER_LENGTH({column_name}))"
    
    def _generate_string_partial(self, column_name, params):
        """Generate Teradata partial masking expression"""
        show_chars = params.get('show_chars', 4)
        mask_char = params.get('mask_char', 'X')
        return f"""
        CASE 
            WHEN CHARACTER_LENGTH({column_name}) <= {show_chars} 
            THEN REPEAT('{mask_char}', CHARACTER_LENGTH({column_name}))
            ELSE REPEAT('{mask_char}', CHARACTER_LENGTH({column_name}) - {show_chars}) || 
                 SUBSTR({column_name}, CHARACTER_LENGTH({column_name}) - {show_chars} + 1)
        END
        """
    
    def _generate_number_range(self, column_name, params):
        """Generate Teradata random number in range"""
        min_val = params.get('min_value', 0)
        max_val = params.get('max_value', 100000)
        return f"CAST(RANDOM(1, 1000000) * ({max_val} - {min_val}) / 1000000 + {min_val} AS INTEGER)"
    
    def _generate_date_shift(self, column_name, params):
        """Generate Teradata date shifting expression"""
        shift_days = params.get('shift_days', 30)
        return f"{column_name} + INTERVAL '{shift_days}' DAY"
    
    def _categorize_data_type(self, data_type):
        """Categorize Teradata data type into STRING, NUMBER, or DATE"""
        data_type_upper = data_type.upper()
        
        if any(t in data_type_upper for t in ['CHAR', 'VARCHAR', 'CLOB', 'TEXT']):
            return 'STRING'
        elif any(t in data_type_upper for t in ['INT', 'DECIMAL', 'NUMERIC', 'FLOAT', 'DOUBLE']):
            return 'NUMBER'
        elif any(t in data_type_upper for t in ['DATE', 'TIME', 'TIMESTAMP']):
            return 'DATE'
        else:
            return 'UNKNOWN'
```

---

### **Phase 5: Worker Process Enhancement**

#### 5.1 Modify Worker Class
**File**: `local/classes/worker.py`

**Purpose**: Ensure masking is applied during data extraction and no unmasked data is stored

**Key Changes**:
1. Add masking validation in `__execute_unload_task` method
2. Integrate Teradata masking SQL generation
3. Add masking status to task logging and results

```python
def __execute_unload_task(self, task):
    """Enhanced unload task execution with masking support"""
    
    # Existing code for task setup...
    
    # Add masking validation and application for Teradata
    if self.system_type.upper() == 'TERADATA':
        original_query = task['query']
        
        # Apply masking transformations
        masking_generator = TeradataMaskingGenerator()
        masked_query = self.__apply_teradata_masking(
            original_query, 
            task.get('object_id'),
            masking_generator
        )
        
        if masked_query != original_query:
            task['query'] = masked_query
            self.logger.info(f"Applied masking to Teradata query for task {task['task_id']}")
            self.logger.debug(f"Original query: {original_query}")
            self.logger.debug(f"Masked query: {masked_query}")
        else:
            self.logger.debug(f"No masking applied for task {task['task_id']}")
    
    # Continue with existing unload logic...
    # Ensure all data written to files is already masked
    
def __apply_teradata_masking(self, query, object_id, masking_generator):
    """Apply masking transformations to Teradata query"""
    
    # Get masking configuration from repository
    masking_config = self.__get_masking_config(object_id)
    
    if not masking_config:
        return query
    
    # Parse and transform SELECT clause
    # Replace column references with masked expressions
    # Return modified query
    
    return modified_query

def __get_masking_config(self, object_id):
    """Retrieve masking configuration for object columns"""
    
    try:
        with Repository(self.repository_details, self.logger) as repository:
            masking_info = repository.execute_query(f"""
                SELECT 
                    column_name, 
                    masking_function_type, 
                    masking_parameters,
                    masking_seed,
                    data_type
                FROM dmva_column_info 
                WHERE object_id = {object_id} 
                AND masking_enabled = TRUE
            """)
            return masking_info
    except Exception as e:
        self.logger.error(f"Failed to retrieve masking configuration: {str(e)}")
        return []
```

**Security Enhancements**:
- Add validation that no unmasked data reaches temporary files
- Include masking status in task completion reports
- Log masking operations for audit trail

---

### **Phase 6: Configuration and Management Tools**

#### 6.1 Create Masking Management Views
**New File**: `snowflake/07_Views/dmva_masking_summary.sql`

**Purpose**: Provide comprehensive visibility into masking configuration

```sql
CREATE OR REPLACE VIEW dmva_masking_summary AS
SELECT 
    s.system_name,
    s.system_type,
    oi.database_name,
    oi.schema_name,
    oi.object_name,
    ci.column_name,
    ci.data_type,
    ci.masking_enabled,
    ci.masking_function_type,
    ci.masking_parameters,
    ci.masking_seed,
    CASE 
        WHEN ci.masking_enabled = TRUE THEN 'MASKED'
        ELSE 'UNMASKED'
    END AS masking_status,
    ci.created_ts,
    ci.updated_ts
FROM dmva_systems s
JOIN dmva_object_info oi ON s.system_id = oi.system_id
JOIN dmva_column_info ci ON oi.object_id = ci.object_id
WHERE s.system_type = 'TERADATA'
AND s.is_source = TRUE
ORDER BY s.system_name, oi.database_name, oi.schema_name, oi.object_name, ci.ordinal_position;

CREATE OR REPLACE VIEW dmva_masking_coverage AS
SELECT 
    system_name,
    database_name,
    schema_name,
    object_name,
    COUNT(*) AS total_columns,
    SUM(CASE WHEN masking_enabled = TRUE THEN 1 ELSE 0 END) AS masked_columns,
    ROUND(
        (SUM(CASE WHEN masking_enabled = TRUE THEN 1 ELSE 0 END) * 100.0) / COUNT(*), 
        2
    ) AS masking_coverage_percent
FROM dmva_masking_summary
GROUP BY system_name, database_name, schema_name, object_name
ORDER BY system_name, database_name, schema_name, object_name;
```

#### 6.2 Create Masking Validation Procedures
**New File**: `snowflake/08_All_Other_Procedures/dmva_validate_masking.sql`

**Purpose**: Validate masking configuration and identify potential issues

```sql
CREATE OR REPLACE PROCEDURE dmva_validate_masking_config()
RETURNS TABLE(validation_result VARCHAR, details VARCHAR)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'validate_masking'
AS $$
def validate_masking(session):
    """Comprehensive validation of masking configuration"""
    
    results = []
    
    # 1. Check for non-Teradata systems with masking enabled
    non_teradata_masked = session.sql("""
        SELECT DISTINCT s.system_name, s.system_type
        FROM dmva_systems s
        JOIN dmva_object_info oi ON s.system_id = oi.system_id
        JOIN dmva_column_info ci ON oi.object_id = ci.object_id
        WHERE ci.masking_enabled = TRUE
        AND s.system_type != 'TERADATA'
    """).collect()
    
    for row in non_teradata_masked:
        results.append((
            'ERROR', 
            f"Masking enabled on non-Teradata system: {row['SYSTEM_NAME']} ({row['SYSTEM_TYPE']})"
        ))
    
    # 2. Check for unsupported data type and masking type combinations
    invalid_combinations = session.sql("""
        SELECT 
            s.system_name,
            oi.object_name,
            ci.column_name,
            ci.data_type,
            ci.masking_function_type
        FROM dmva_systems s
        JOIN dmva_object_info oi ON s.system_id = oi.system_id
        JOIN dmva_column_info ci ON oi.object_id = ci.object_id
        WHERE ci.masking_enabled = TRUE
        AND (
            (ci.data_type ILIKE '%CHAR%' AND ci.masking_function_type NOT IN ('STRING')) OR
            (ci.data_type ILIKE '%INT%' AND ci.masking_function_type NOT IN ('NUMBER')) OR
            (ci.data_type ILIKE '%DATE%' AND ci.masking_function_type NOT IN ('DATE'))
        )
    """).collect()
    
    for row in invalid_combinations:
        results.append((
            'WARNING',
            f"Potential data type mismatch: {row['COLUMN_NAME']} ({row['DATA_TYPE']}) using {row['MASKING_FUNCTION_TYPE']} masking"
        ))
    
    # 3. Check for missing masking parameters
    missing_params = session.sql("""
        SELECT 
            s.system_name,
            oi.object_name,
            ci.column_name,
            ci.masking_function_type
        FROM dmva_systems s
        JOIN dmva_object_info oi ON s.system_id = oi.system_id
        JOIN dmva_column_info ci ON oi.object_id = ci.object_id
        WHERE ci.masking_enabled = TRUE
        AND ci.masking_parameters IS NULL
    """).collect()
    
    for row in missing_params:
        results.append((
            'INFO',
            f"Using default parameters for {row['COLUMN_NAME']} with {row['MASKING_FUNCTION_TYPE']} masking"
        ))
    
    # 4. Summary statistics
    summary = session.sql("""
        SELECT 
            COUNT(DISTINCT s.system_name) AS teradata_systems,
            COUNT(DISTINCT CONCAT(oi.database_name, '.', oi.schema_name, '.', oi.object_name)) AS total_objects,
            COUNT(*) AS total_columns,
            SUM(CASE WHEN ci.masking_enabled = TRUE THEN 1 ELSE 0 END) AS masked_columns
        FROM dmva_systems s
        JOIN dmva_object_info oi ON s.system_id = oi.system_id
        JOIN dmva_column_info ci ON oi.object_id = ci.object_id
        WHERE s.system_type = 'TERADATA' AND s.is_source = TRUE
    """).collect()[0]
    
    results.append((
        'INFO',
        f"Masking Summary: {summary['MASKED_COLUMNS']}/{summary['TOTAL_COLUMNS']} columns masked across {summary['TOTAL_OBJECTS']} objects in {summary['TERADATA_SYSTEMS']} Teradata systems"
    ))
    
    return results
$$;
```

---

### **Phase 7: Documentation and Examples**

#### 7.1 Create Comprehensive Masking Documentation
**New File**: `documentation/Data_Masking.md`

```markdown
# DMVA In-Flight Data Masking

## Overview
This feature provides in-flight data masking for Teradata sources, ensuring sensitive data is masked before any storage or transmission during the migration process.

## Key Features
- **Teradata-Only**: Masking is exclusively available for Teradata source systems
- **In-Flight Processing**: Data is masked during extraction, never stored unmasked
- **Three Data Types**: Dedicated functions for strings, numbers, and dates
- **Column-Level Control**: Granular masking configuration per column
- **Deterministic Options**: Hash-based masking preserves referential integrity

## Configuration Guide

### 1. Basic Column Masking
Configure masking for specific columns:

```sql
CALL dmva_configure_masking(
    'TERADATA_PROD',
    {'HR_DB': {'EMPLOYEES': ['EMPLOYEES']}},
    {
        'SSN': {'type': 'STRING', 'method': 'PARTIAL', 'params': {'show_chars': 4}},
        'SALARY': {'type': 'NUMBER', 'method': 'NOISE', 'params': {'noise_percent': 15}}
    }
);
```

### 2. Pattern-Based Configuration
Set up rules that automatically apply to matching columns:

```sql
INSERT INTO dmva_masking_config VALUES
('TERADATA_PROD', '%', '%', '%', '%SSN%', 'STRING', 'STRING', '{"method": "PARTIAL", "params": {"show_chars": 4}}', 1, TRUE),
('TERADATA_PROD', '%', '%', '%', '%SALARY%', 'NUMBER', 'NUMBER', '{"method": "NOISE", "params": {"noise_percent": 10}}', 2, TRUE);

CALL dmva_apply_masking_rules();
```

## Masking Methods

### String Masking
- **HASH**: `{'method': 'HASH', 'params': {'seed': 'company_salt'}}`
- **PARTIAL**: `{'method': 'PARTIAL', 'params': {'show_chars': 4, 'mask_char': 'X'}}`
- **RANDOM**: `{'method': 'RANDOM', 'params': {'charset': 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'}}`
- **REDACT**: `{'method': 'REDACT'}`

### Number Masking
- **RANGE**: `{'method': 'RANGE', 'params': {'min_value': 50000, 'max_value': 150000}}`
- **NOISE**: `{'method': 'NOISE', 'params': {'noise_percent': 15}}`
- **BUCKET**: `{'method': 'BUCKET', 'params': {'buckets': [25000, 50000, 75000, 100000]}}`
- **HASH**: `{'method': 'HASH', 'params': {'seed': 'salary_seed'}}`

### Date Masking
- **RANGE**: `{'method': 'RANGE', 'params': {'start_date': '2020-01-01', 'end_date': '2023-12-31'}}`
- **SHIFT**: `{'method': 'SHIFT', 'params': {'shift_days': 30}}`
- **YEAR_ONLY**: `{'method': 'YEAR_ONLY'}`
- **MONTH_ONLY**: `{'method': 'MONTH_ONLY'}`

## Monitoring and Validation

### Check Masking Coverage
```sql
SELECT * FROM dmva_masking_coverage 
WHERE masking_coverage_percent < 100;
```

### Validate Configuration
```sql
CALL dmva_validate_masking_config();
```

### View Masking Summary
```sql
SELECT * FROM dmva_masking_summary 
WHERE system_name = 'TERADATA_PROD';
```

## Best Practices

1. **Use Deterministic Masking for Joins**: Use HASH methods for columns involved in joins
2. **Test Masking Logic**: Validate masking results before production deployment
3. **Document Masking Policies**: Maintain clear documentation of what data is masked and how
4. **Regular Validation**: Periodically run validation procedures to ensure configuration integrity
5. **Performance Testing**: Monitor performance impact of masking on large tables

## Troubleshooting

### Common Issues
1. **Masking Not Applied**: Verify system is Teradata and masking_enabled = TRUE
2. **Performance Impact**: Consider simpler masking methods for very large tables
3. **Data Type Mismatches**: Ensure masking function type matches column data type

### Diagnostic Queries
```sql
-- Check if masking is configured
SELECT COUNT(*) FROM dmva_column_info WHERE masking_enabled = TRUE;

-- Verify Teradata-only restriction
SELECT DISTINCT s.system_type 
FROM dmva_systems s
JOIN dmva_object_info oi ON s.system_id = oi.system_id
JOIN dmva_column_info ci ON oi.object_id = ci.object_id
WHERE ci.masking_enabled = TRUE;
```
```

#### 7.2 Create Configuration Examples
**New File**: `examples/masking_configuration.sql`

```sql
-- DMVA In-Flight Data Masking Configuration Examples

-- Example 1: Healthcare Data Masking
CALL dmva_configure_masking(
    'TERADATA_HEALTHCARE',
    {'PATIENT_DB': {'RECORDS': ['PATIENTS', 'VISITS', 'BILLING']}},
    {
        'SSN': {'type': 'STRING', 'method': 'HASH', 'params': {'seed': 'healthcare_salt'}},
        'PATIENT_NAME': {'type': 'STRING', 'method': 'REDACT'},
        'DOB': {'type': 'DATE', 'method': 'SHIFT', 'params': {'shift_days': 90}},
        'PHONE': {'type': 'STRING', 'method': 'PARTIAL', 'params': {'show_chars': 4}},
        'BILLING_AMOUNT': {'type': 'NUMBER', 'method': 'NOISE', 'params': {'noise_percent': 20}}
    }
);

-- Example 2: Financial Services Masking
CALL dmva_configure_masking(
    'TERADATA_FINANCE',
    {'BANKING_DB': {'ACCOUNTS': ['CUSTOMERS', 'TRANSACTIONS', 'LOANS']}},
    {
        'ACCOUNT_NUMBER': {'type': 'STRING', 'method': 'HASH', 'params': {'seed': 'account_salt'}},
        'CUSTOMER_NAME': {'type': 'STRING', 'method': 'RANDOM', 'params': {'charset': 'ABCDEFGHIJKLMNOPQRSTUVWXYZ '}},
        'TRANSACTION_AMOUNT': {'type': 'NUMBER', 'method': 'BUCKET', 'params': {'buckets': [100, 500, 1000, 5000, 10000]}},
        'TRANSACTION_DATE': {'type': 'DATE', 'method': 'MONTH_ONLY'},
        'CREDIT_SCORE': {'type': 'NUMBER', 'method': 'RANGE', 'params': {'min_value': 300, 'max_value': 850}}
    }
);

-- Example 3: Retail Customer Data Masking
CALL dmva_configure_masking(
    'TERADATA_RETAIL',
    {'RETAIL_DB': {'CUSTOMER_DATA': ['CUSTOMERS', 'ORDERS', 'PREFERENCES']}},
    {
        'EMAIL': {'type': 'STRING', 'method': 'PARTIAL', 'params': {'show_chars': 0, 'mask_char': 'user@domain.com'}},
        'CUSTOMER_ID': {'type': 'STRING', 'method': 'HASH', 'params': {'seed': 'customer_salt'}},
        'ORDER_VALUE': {'type': 'NUMBER', 'method': 'NOISE', 'params': {'noise_percent': 25}},
        'BIRTH_DATE': {'type': 'DATE', 'method': 'YEAR_ONLY'},
        'ZIP_CODE': {'type': 'STRING', 'method': 'PARTIAL', 'params': {'show_chars': 2}}
    }
);

-- Example 4: Pattern-Based Bulk Configuration
-- Configure masking rules that apply automatically to matching columns

INSERT INTO dmva_masking_config VALUES
-- SSN patterns
('TERADATA_PROD', '%', '%', '%', '%SSN%', 'STRING', 'STRING', '{"method": "HASH", "params": {"seed": "ssn_salt"}}', 1, TRUE),
('TERADATA_PROD', '%', '%', '%', '%SOCIAL%', 'STRING', 'STRING', '{"method": "HASH", "params": {"seed": "ssn_salt"}}', 1, TRUE),

-- Email patterns  
('TERADATA_PROD', '%', '%', '%', '%EMAIL%', 'STRING', 'STRING', '{"method": "PARTIAL", "params": {"show_chars": 0, "replacement": "user@domain.com"}}', 2, TRUE),

-- Phone patterns
('TERADATA_PROD', '%', '%', '%', '%PHONE%', 'STRING', 'STRING', '{"method": "PARTIAL", "params": {"show_chars": 4}}', 3, TRUE),

-- Salary/Income patterns
('TERADATA_PROD', '%', '%', '%', '%SALARY%', 'NUMBER', 'NUMBER', '{"method": "NOISE", "params": {"noise_percent": 15}}', 4, TRUE),
('TERADATA_PROD', '%', '%', '%', '%INCOME%', 'NUMBER', 'NUMBER', '{"method": "BUCKET", "params": {"buckets": [25000, 50000, 75000, 100000, 150000]}}', 4, TRUE),

-- Date of Birth patterns
('TERADATA_PROD', '%', '%', '%', '%DOB%', 'DATE', 'DATE', '{"method": "YEAR_ONLY"}', 5, TRUE),
('TERADATA_PROD', '%', '%', '%', '%BIRTH%', 'DATE', 'DATE', '{"method": "SHIFT", "params": {"shift_days": 30}}', 5, TRUE);

-- Apply the pattern-based rules
CALL dmva_apply_masking_rules();

-- Example 5: Validation and Monitoring
-- Check masking coverage
SELECT 
    system_name,
    object_name,
    total_columns,
    masked_columns,
    masking_coverage_percent
FROM dmva_masking_coverage
WHERE masking_coverage_percent < 100
ORDER BY masking_coverage_percent ASC;

-- Validate configuration
CALL dmva_validate_masking_config();

-- Expected: Clear visibility into masking status and any configuration issues
```

---

### **Phase 8: Testing and Validation**

#### 8.1 Create Test Framework
**New File**: `snowflake/tests/test_masking.sql`

```sql
-- DMVA Masking Test Suite

-- Test 1: Masking Function Validation
SELECT 
    'String HASH Test' AS test_name,
    dmva_mask_string('123-45-6789', 'HASH', '{"seed": "test_seed"}') AS result,
    CASE 
        WHEN dmva_mask_string('123-45-6789', 'HASH', '{"seed": "test_seed"}') != '123-45-6789' 
        THEN 'PASS' 
        ELSE 'FAIL' 
    END AS status;

SELECT 
    'String PARTIAL Test' AS test_name,
    dmva_mask_string('123-45-6789', 'PARTIAL', '{"show_chars": 4}') AS result,
    CASE 
        WHEN dmva_mask_string('123-45-6789', 'PARTIAL', '{"show_chars": 4}') LIKE '%6789' 
        THEN 'PASS' 
        ELSE 'FAIL' 
    END AS status;

SELECT 
    'Number RANGE Test' AS test_name,
    dmva_mask_number(75000, 'RANGE', '{"min_value": 50000, "max_value": 100000}') AS result,
    CASE 
        WHEN dmva_mask_number(75000, 'RANGE', '{"min_value": 50000, "max_value": 100000}') BETWEEN 50000 AND 100000 
        THEN 'PASS' 
        ELSE 'FAIL' 
    END AS status;

SELECT 
    'Date SHIFT Test' AS test_name,
    dmva_mask_date('2023-01-15'::DATE, 'SHIFT', '{"shift_days": 30}') AS result,
    CASE 
        WHEN dmva_mask_date('2023-01-15'::DATE, 'SHIFT', '{"shift_days": 30}') != '2023-01-15'::DATE 
        THEN 'PASS' 
        ELSE 'FAIL' 
    END AS status;

-- Test 2: Teradata-Only Restriction
-- This should fail for non-Teradata systems
CALL dmva_configure_masking(
    'ORACLE_SYSTEM',  -- Non-Teradata system
    {'TEST_DB': {'TEST_SCHEMA': ['TEST_TABLE']}},
    {'TEST_COLUMN': {'type': 'STRING', 'method': 'HASH'}}
);

-- Test 3: Configuration Validation
CALL dmva_validate_masking_config();

-- Test 4: Performance Baseline
-- Measure query performance with and without masking
SELECT 
    'Performance Test' AS test_name,
    COUNT(*) AS record_count,
    CURRENT_TIMESTAMP() AS test_timestamp
FROM dmva_column_info
WHERE masking_enabled = TRUE;
```

#### 8.2 Integration Test Plan

**Test Scenarios**:

1. **End-to-End Masking Workflow**
   - Configure masking for test Teradata system
   - Execute unload task
   - Verify masked data in output files
   - Confirm no unmasked data in temporary locations

2. **Data Type Coverage**
   - Test all supported data types (VARCHAR, INTEGER, DATE, etc.)
   - Verify appropriate masking functions are applied
   - Test edge cases (NULL values, empty strings, etc.)

3. **Performance Impact Assessment**
   - Baseline performance without masking
   - Measure performance with various masking methods
   - Identify performance bottlenecks
   - Validate <10% overhead requirement

4. **Security Validation**
   - Audit all temporary file locations
   - Verify no unmasked data persists anywhere
   - Test masking consistency across multiple runs
   - Validate deterministic masking preserves joins

5. **Error Handling**
   - Test invalid masking configurations
   - Verify graceful handling of unsupported data types
   - Test system behavior with missing parameters
   - Validate error messages and logging

---

## 🔧 **Implementation Considerations**

### **Security Requirements**

#### Data Protection
- **Zero Unmasked Storage**: Implement validation checks to ensure no unmasked sensitive data is written to any temporary location
- **Audit Trail**: Log all masking operations with timestamps, user context, and configuration details
- **Access Control**: Restrict masking configuration to authorized users only

#### Teradata-Only Enforcement
- **System Validation**: Add checks in all masking procedures to verify system type is Teradata
- **Configuration Guards**: Prevent masking configuration on non-Teradata systems
- **Runtime Validation**: Double-check system type during query execution

### **Performance Considerations**

#### Query Optimization
- **Function Efficiency**: Ensure masking functions are optimized for large datasets
- **Pushdown Optimization**: Generate Teradata-native SQL to leverage database optimization
- **Batch Processing**: Maintain existing parallelization capabilities

#### Memory Management
- **Worker Process Impact**: Monitor memory usage with masking transformations
- **Streaming Processing**: Ensure masking works with streaming data processing
- **Resource Allocation**: Consider additional CPU/memory requirements for masking operations

#### Performance Monitoring
- **Baseline Metrics**: Establish performance baselines before masking implementation
- **Continuous Monitoring**: Track query execution times with masking enabled
- **Alerting**: Set up alerts for performance degradation beyond acceptable thresholds

### **Compatibility and Integration**

#### Teradata Version Support
- **Version Testing**: Test with different Teradata versions (14.x, 15.x, 16.x, 17.x)
- **SQL Dialect Compatibility**: Ensure generated SQL works across Teradata versions
- **Feature Availability**: Handle version-specific function availability

#### Data Type Support
- **Comprehensive Coverage**: Support all common Teradata data types
- **Edge Case Handling**: Handle special data types (JSON, XML, BLOB, etc.)
- **Type Conversion**: Ensure masking preserves data type characteristics

#### Existing Workflow Integration
- **Backward Compatibility**: Ensure existing DMVA workflows continue to work
- **Configuration Migration**: Provide tools to migrate existing configurations
- **Gradual Rollout**: Support phased implementation across different systems

### **Operational Considerations**

#### Configuration Management
- **Version Control**: Track masking configuration changes
- **Environment Promotion**: Support dev/test/prod configuration promotion
- **Backup and Recovery**: Include masking configuration in backup procedures

#### Monitoring and Alerting
- **Masking Status**: Monitor masking application success/failure rates
- **Performance Metrics**: Track masking overhead and performance impact
- **Configuration Drift**: Alert on unauthorized masking configuration changes

#### Documentation and Training
- **User Guides**: Comprehensive documentation for different user roles
- **Training Materials**: Hands-on training for administrators and users
- **Best Practices**: Document recommended masking strategies for different data types

---

## 📋 **Implementation Timeline and Phases**

### **Phase 1: Foundation (Weeks 1-2)**
- Database schema extensions
- Core masking functions development
- Basic configuration procedures

**Deliverables**:
- Extended `dmva_column_info` table
- New `dmva_masking_config` table
- Three core masking functions (string, number, date)
- Basic configuration procedures

**Success Criteria**:
- All database objects created successfully
- Masking functions pass unit tests
- Basic configuration workflow functional

### **Phase 2: Query Generation (Weeks 3-4)**
- Teradata SQL generation enhancement
- Query transformation logic
- Integration with existing unload task generation

**Deliverables**:
- `TeradataMaskingGenerator` class
- Modified `dmva_get_unload_tasks` procedure
- Teradata-specific masking SQL expressions

**Success Criteria**:
- Masked queries generate correctly
- Teradata-only restriction enforced
- Integration with existing workflow seamless

### **Phase 3: Worker Integration (Weeks 5-6)**
- Worker process enhancement
- Security validation implementation
- Performance optimization

**Deliverables**:
- Enhanced `Worker` class with masking support
- Security validation checks
- Performance monitoring integration

**Success Criteria**:
- No unmasked data in temporary files
- Masking applied consistently
- Performance impact within acceptable limits

### **Phase 4: Management Tools (Weeks 7-8)**
- Management views and procedures
- Validation and monitoring tools
- Configuration utilities

**Deliverables**:
- Masking summary views
- Validation procedures
- Configuration management tools

**Success Criteria**:
- Complete visibility into masking configuration
- Comprehensive validation capabilities
- User-friendly management interface

### **Phase 5: Documentation and Testing (Weeks 9-10)**
- Comprehensive documentation
- Test framework development
- Integration testing

**Deliverables**:
- Complete documentation suite
- Automated test framework
- Integration test results

**Success Criteria**:
- All documentation complete and accurate
- Test framework covers all scenarios
- Integration tests pass successfully

### **Phase 6: Deployment and Validation (Weeks 11-12)**
- Production deployment preparation
- Performance validation
- Security audit

**Deliverables**:
- Production-ready implementation
- Performance validation report
- Security audit results

**Success Criteria**:
- Production deployment successful
- Performance requirements met
- Security requirements validated

---

## 🎯 **Success Criteria and Acceptance Tests**

### **Functional Requirements**
- ✅ **Column-Level Masking**: Users can specify individual columns for masking
- ✅ **Three Data Type Functions**: Dedicated masking for strings, numbers, and dates
- ✅ **Teradata-Only Application**: Masking only applies to Teradata source systems
- ✅ **No Unmasked Storage**: Data is never stored unmasked in any location
- ✅ **Configuration Flexibility**: Support for both manual and pattern-based configuration

### **Performance Requirements**
- ✅ **Minimal Overhead**: Masking adds <10% to query execution time
- ✅ **Scalability**: Handles large tables (100M+ rows) without significant degradation
- ✅ **Parallel Processing**: Maintains existing parallelization capabilities
- ✅ **Memory Efficiency**: No significant increase in memory usage

### **Security Requirements**
- ✅ **Data Protection**: No sensitive data exposed in logs, temporary files, or error messages
- ✅ **Access Control**: Masking configuration restricted to authorized users
- ✅ **Audit Trail**: Complete logging of masking operations and configuration changes
- ✅ **Compliance**: Meets data privacy regulations (GDPR, HIPAA, etc.)

### **Operational Requirements**
- ✅ **Backward Compatibility**: Existing DMVA workflows continue to function
- ✅ **Monitoring**: Comprehensive visibility into masking status and performance
- ✅ **Documentation**: Complete user and administrator documentation
- ✅ **Support**: Clear troubleshooting guides and error resolution procedures

### **Acceptance Test Scenarios**

#### Test 1: Basic Masking Functionality
```sql
-- Configure masking for test table
CALL dmva_configure_masking(
    'TEST_TERADATA',
    {'TEST_DB': {'TEST_SCHEMA': ['CUSTOMER_DATA']}},
    {
        'SSN': {'type': 'STRING', 'method': 'PARTIAL', 'params': {'show_chars': 4}},
        'SALARY': {'type': 'NUMBER', 'method': 'NOISE', 'params': {'noise_percent': 10}},
        'DOB': {'type': 'DATE', 'method': 'SHIFT', 'params': {'shift_days': 30}}
    }
);

-- Execute unload task and verify masking applied
-- Expected: All specified columns are masked in output
```

#### Test 2: Teradata-Only Restriction
```sql
-- Attempt to configure masking on Oracle system
CALL dmva_configure_masking(
    'ORACLE_SYSTEM',
    {'TEST_DB': {'TEST_SCHEMA': ['TEST_TABLE']}},
    {'TEST_COLUMN': {'type': 'STRING', 'method': 'HASH'}}
);

-- Expected: Error message indicating masking only supported for Teradata
```

#### Test 3: Performance Validation
```sql
-- Measure performance with and without masking
-- Expected: <10% performance overhead with masking enabled
```

#### Test 4: Security Validation
```bash
# Check for unmasked data in temporary files
find /tmp -name "*.csv" -exec grep -l "123-45-6789" {} \;
# Expected: No unmasked SSN found in any temporary files
```

#### Test 5: Configuration Management
```sql
-- View masking coverage
SELECT * FROM dmva_masking_coverage WHERE masking_coverage_percent < 100;

-- Validate configuration
CALL dmva_validate_masking_config();

-- Expected: Clear visibility into masking status and any configuration issues
```

---

## 🚀 **Deployment Strategy**

### **Development Environment Setup**
1. Create dedicated development branch
2. Set up test Teradata environment
3. Implement core functionality
4. Conduct unit testing

### **Testing Environment Validation**
1. Deploy to testing environment
2. Execute comprehensive test suite
3. Performance benchmarking
4. Security validation

### **Production Deployment**
1. Gradual rollout to pilot systems
2. Monitor performance and functionality
3. Full production deployment
4. Post-deployment validation

### **Rollback Plan**
1. Database schema rollback scripts
2. Configuration backup and restore
3. Worker process rollback procedures
4. Communication plan for rollback scenarios

---

This comprehensive implementation plan provides a roadmap for successfully implementing in-flight data masking capabilities in DMVA V2 while maintaining security, performance, and operational requirements. The phased approach ensures systematic development and validation of each component before integration into the production environment. 