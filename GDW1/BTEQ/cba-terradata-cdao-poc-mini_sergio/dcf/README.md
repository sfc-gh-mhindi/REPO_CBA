# 🚀 GDW1 dbt Migration Framework
## **DataStage to dbt Migration using DCF Control Framework**

### 📋 Overview

The **GDW1 dbt Migration Framework** modernizes legacy DataStage ETL pipelines to dbt using the **DCF (dbt Control Framework)** on Snowflake. This implementation provides enterprise-grade data processing with comprehensive validation, error handling, and audit capabilities.

**Key Features:**
- 🏗️ **Multi-layer Architecture**: Staging → Intermediate → Marts pattern
- 🔍 **Comprehensive Validation**: Header/trailer validation, business date validation, data quality checks
- 🎯 **CCODS Focus**: Complete BCFINSG stream implementation with EBCDIC processing
- 📊 **DCF Integration**: Stream management, process tracking, and audit logging
- ⚡ **Type 2 SCD**: Historical data preservation with Iceberg cloud tables
- 🛡️ **Error Management**: Centralized error logging and recovery procedures

---

## 🏗️ Architecture Overview

The GDW1 dbt Migration Framework implements a modern, cloud-native ETL architecture that replaces legacy DataStage processes. The framework leverages the proven DCF (dbt Control Framework) from GDW2 while adding GDW1-specific capabilities for EBCDIC processing and complex validation requirements.

*For detailed architecture diagrams, see the [CCODS-specific diagram](#ccods-architecture-overview) below and the [complete architecture diagram](#-complete-architecture-diagram---both-streams) at the end of this document.*

---

## 🎯 Framework Capabilities

### **GDW1-Specific Implementations**

The following capabilities were specifically developed for GDW1 migration requirements:

#### **🔍 Advanced Validation Framework**

The GDW1 validation framework implements a comprehensive multi-layer validation approach that ensures data integrity at every stage of processing:

##### **📋 File Structure Validation**
**Purpose**: Ensures file integrity before processing begins
- **Header Validation (FVL0001)**: Verifies header record presence and metadata completeness
- **Date Validation (FVL0002)**: Compares file processing date against active business date  
- **Filename Validation (FVL0003)**: Optional pattern matching for naming conventions
- **Trailer Validation (FVL0006)**: Confirms trailer record presence and metadata
- **Record Count Validation (FVL0005)**: **CRITICAL** - Trailer count vs actual staging table count

```sql
-- Header/trailer validation in pre-hook
pre_hook=[
  "{{ validate_header_trailer('CSEL4_CPL_BUS_APP', 
       var('staging_database') ~ '.' ~ var('staging_schema') ~ '.cse_cpl_bus_app') }}"
]
```

**Error Handling**: Immediately fails dbt run if validation detects issues:
```sql
{% if not validation_passed %}
  {{ exceptions.raise_compiler_error("Header/Trailer Validation Failed: " ~ status_message) }}
{% endif %}
```

##### **🏛️ Infrastructure Validation**
**Purpose**: Ensures proper stream state and prevents concurrent execution
- **Business Date Consistency**: Validates exactly one open business date per stream
- **Stream State Management**: Prevents multiple concurrent runs of the same stream
- **DCF Prerequisites**: Verifies all required control tables are accessible and properly configured

```sql
-- Business date validation example
SELECT count(*) as open_count
FROM DCF_T_STRM_BUS_DT
WHERE STRM_NAME = 'BCFINSG' AND PROCESSING_FLAG = 1
-- Must equal exactly 1, or validation fails
```

##### **🔬 Data Quality Validation**
**Purpose**: Field-level validation ensuring data meets business requirements on already-ingested Snowflake tables

**Date Field Validation**: Validates converted date fields using custom UDFs
- **17 Date Fields**: Each BCFINSG record has 17 date fields requiring validation
- **UDF Integration**: Reuses existing DataStage validation logic via `fn_is_valid_dt` UDF
- **Post-Ingestion Validation**: Works on Snowflake tables after EBCDIC conversion is complete

```sql
-- Date validation using custom UDF on converted data
{{ dcf_database_ref() }}.fn_is_valid_dt(BCF_DT_FIRST_TRANS) as valid_first_trans,
{{ dcf_database_ref() }}.fn_is_valid_dt(BCF_DT_LAST_PAYMENT) as valid_last_payment,
-- ... validation for all 17 date fields

-- Overall record validation
CASE WHEN (
  NOT valid_first_trans OR
  NOT valid_last_payment OR 
  -- ... other validations
) THEN TRUE ELSE FALSE END as has_error
```

**Multi-Stream Support**: Handles different ingested data formats
- **BCFINSG Stream**: Processes tables from converted EBCDIC files  
- **CSEL4 Stream**: Processes tables from CSV files
- **Unified Error Handling**: Same error table structure for both streams

**Data Type and Business Rule Validation**:
- **NULL Value Checks**: Mandatory field validation
- **Data Type Consistency**: Format and length validation  
- **Domain Value Validation**: Acceptable value range checking
- **Cross-Field Validation**: Logical relationships between fields
- **Custom Business Rules**: Industry-specific validation logic

##### **🔗 Reference Data Validation**
**Purpose**: Ensures referential integrity and lookup completeness

**Dynamic Lookup Tables**: Time-aware reference data with effective dating
```sql
-- Reference table with current effective records
WITH business_date AS (
  SELECT BUS_DT as current_business_date 
  FROM DCF_T_STRM_BUS_DT 
  WHERE STRM_NAME = '{{ var("run_stream") }}' AND PROCESSING_FLAG = 1
),
current_effective AS (
  SELECT pl_pack_cat_id, pdct_n,
    CASE 
      WHEN current_business_date BETWEEN efft_d AND expy_d 
      THEN TRUE ELSE FALSE 
    END AS is_current
  FROM source_data s
  CROSS JOIN business_date bd
)
```

**Foreign Key and Orphan Detection**:
- **Department ID Validation**: Ensures valid branch/department references
- **Product ID Validation**: Confirms product package mappings exist
- **Effective Date Checking**: Validates lookups are current for processing date
- **Orphaned Record Detection**: Identifies records without valid references

**NULL Handling Strategy**: DataStage behavior replication
```sql
-- Filter out records with NULL foreign keys (matches DataStage behavior)
WHERE s.pl_package_cat_id IS NOT NULL
```

##### **⚡ Validation Execution Strategy**

**Fail-Fast vs Log-and-Continue**:
- **🛑 Fail-Fast**: File structure, business date, record count mismatches
- **📝 Log-and-Continue**: Individual record data quality issues
- **🔄 Conditional Processing**: Downstream models only run if validation passes

**Performance Optimization**:
- **View Materialization**: Validation models as views to avoid data duplication
- **Selective Column Reading**: Only reads columns needed for validation
- **Early Filtering**: WHERE clauses applied before expensive operations

**Validation Dependencies**:
```sql
-- Dependency check ensures validation completes before transformation
error_check_dependency AS (
  SELECT COUNT(*) as error_count
  FROM {{ ref('int_validate_bcfinsg') }}
)
```

#### **🔄 Stream Process Control**

**Purpose**: Orchestrates multi-stream processing with proper dependencies and lifecycle management

**Tag-Based Stream Execution**: Enables independent stream processing
```bash
# Execute specific streams independently
dbt run --select tag:stream_bcfinsg   # BCFINSG stream only
dbt run --select tag:stream_csel4     # CSEL4 stream only
```

**Process Dependency Management**: Ensures proper execution order within streams
```sql
-- Validation must complete before transformation
error_check_dependency AS (
  SELECT COUNT(*) as error_count
  FROM {{ ref('int_validate_bcfinsg') }}
)
```

**Business Date Synchronization**: Each stream manages its own business date lifecycle
```sql
-- Stream-specific business date lookup
SELECT BUS_DT as current_business_date 
FROM DCF_T_STRM_BUS_DT 
WHERE STRM_NAME = '{{ var("run_stream") }}' AND PROCESSING_FLAG = 1
```

#### **🗂️ Data Transformation & Materialization**

**Type 2 SCD with Iceberg**: Custom materialization for cloud-native historical tracking
```sql
-- Custom materialization for historical data preservation
{{ config(
  materialized='ibrg_cld_table',
  incremental_strategy='truncate-load',
  database=var('target_database'),
  tmp_database=var('dcf_database'),
  tmp_schema=var('dcf_schema')
) }}
```

**Multi-Table Relationships**: Complex stream outputs
- **BCFINSG Stream**: Single target table with comprehensive business logic
- **CSEL4 Stream**: 3 interconnected target tables (appt_dept, appt_pdct, dept_appt)
- **Reference Data Integration**: Dynamic lookup tables with effective dating

**Business Logic Implementation**: Transforms validated data into target structures
```sql
-- Business transformations on validated data
WITH source_data AS (
  SELECT s.*, bd.business_date, pi.process_instance_id as batch_id
  FROM {{ source('gdw1_raw', 'bcfinsg') }} s
  CROSS JOIN business_date bd
  CROSS JOIN process_instance pi
),
transformed AS (
  SELECT 
    TRIM(BCF_ACCOUNT_NO1) as account_number,
    business_date as EFFT_D,
    batch_id as PROS_KEY_EFFT_I,
    -- ... other business transformations
  FROM source_data
)
```

#### **❌ Centralized Error Management**
- **Single Error Repository**: All validation errors flow to centralized `XFM_ERR_DTL` table
- **Context-Rich Error Records**: Error codes, source keys, business dates, and process instances
- **Flexible Error Handling**: Fail-fast for critical errors, log-and-continue for data quality issues

```sql
-- Error record structure
ERR_C: 'DQ001'  -- Error code
ERR_MSG: 'Invalid date field: BCF_DT_FIRST_TRANS'
ERR_SRC_KEY_VAL: BCF_ACCOUNT_NO1  -- Source record identifier
```

#### **📊 dbt Hook Framework Integration**

**Purpose**: Seamless integration of validation and process control into dbt execution lifecycle

**Pre-Hook Orchestration**: Sets up prerequisites before model execution
```sql
pre_hook=[
  "{{ validate_single_open_business_date('BCFINSG') }}",  # Stream state validation
  "{{ validate_header('BCFINSG') }}",                     # File header validation  
  "{{ register_process_instance('PROCESS_NAME', 'BCFINSG') }}", # DCF process tracking
  "{{ err_tbl_reset('BCFINSG', 'PROCESS_NAME') }}"        # Error table cleanup
]
```

**Post-Hook Actions**: Handles results and cleanup after model execution
```sql
post_hook=[
  "{{ load_errors_to_central_table(this, 'BCFINSG', 'PROCESS_NAME') }}", # Error consolidation
  "{{ check_error_and_end_prcs('BCFINSG', 'PROCESS_NAME') }}"            # Process completion
]
```

**Conditional Execution**: Models only run if dependencies are satisfied
```sql
-- Hook-based dependency management
{% if execute %}
  {% set error_count = run_query("SELECT COUNT(*) FROM " ~ ref('int_validate_bcfinsg')) %}
  {% if error_count.rows[0][0] > 0 %}
    {{ exceptions.raise_compiler_error("Validation failed - " ~ error_count.rows[0][0] ~ " errors found") }}
  {% endif %}
{% endif %}
```

### **GDW2 DCF Framework (Inherited)**

The following mature capabilities are inherited from the proven GDW2 implementation:

#### **📊 Stream Lifecycle Management**
- **Stream Registration**: `DCF_T_STRM` table for stream configuration and metadata
- **Business Date Management**: `DCF_T_STRM_BUS_DT` for consistent date handling across streams
- **Stream State Control**: Start, run, and end operations with proper state transitions

```bash
# Inherited DCF operations
dbt run-operation start_stream_op --args '{stream_name: "BCFINSG", business_date: "2024-12-20"}'
dbt run-operation end_stream_op --args '{stream_name: "BCFINSG"}'
```

#### **🔄 Process Instance Tracking**
- **Process Registration**: `DCF_T_PRCS_INST` for granular process execution tracking
- **Execution Hierarchy**: Stream → Process → Step level tracking
- **Performance Metrics**: Start/end timestamps, duration, and status tracking

#### **📝 Comprehensive Audit Logging**
- **Execution Trail**: `DCF_T_EXEC_LOG` captures every significant event
- **Message Classification**: INFO, WARNING, ERROR message types with standardized codes
- **Operational Intelligence**: Full visibility into stream execution patterns

#### **🎛️ Hook Framework Integration**
- **Pre-Hook Validations**: Infrastructure checks before model execution
- **Post-Hook Actions**: Status updates, error handling, and completion tracking
- **Macro Ecosystem**: Reusable validation and control macros

```sql
-- Inherited hook patterns
pre_hook=["{{ validate_single_open_business_date('BCFINSG') }}"]
post_hook=["{{ mark_process_completed('PROCESS_NAME', 'BCFINSG') }}"]
```

### **🔗 Integration Benefits**

#### **📈 Consistency Across GDW1 & GDW2**
- **Unified Operations**: Same operational commands work across both platforms
- **Shared Monitoring**: Common audit tables and reporting mechanisms  
- **Cross-Platform Skills**: Team knowledge transfers between GDW1 and GDW2

#### **🛡️ Proven Reliability**
- **Battle-Tested Framework**: DCF framework already proven in GDW2 production
- **Mature Error Handling**: Established patterns for error recovery and retry logic
- **Operational Excellence**: Known good practices for monitoring and maintenance

#### **⚡ Accelerated Development**
- **Reduced Build Time**: Leveraging existing DCF macros and patterns
- **Lower Risk**: Using proven components reduces implementation risk
- **Faster Adoption**: Teams already familiar with DCF operations

---

## 🔍 CCODS Stream Focus - BCFINSG Implementation

### CCODS Architecture Overview

![CCODS Stream Architecture](../../../others/resources/cods.svg)

### Stream Configuration
- **Stream ID**: 1490
- **Stream Name**: BCFINSG (Bank Consumer Finance Segment)
- **Business Domain**: FINANCE 
- **Target**: `ps_cld_rw.pdsrccs.plan_baln_segm_mstr`
- **Source**: EBCDIC files converted to Snowflake tables
- **Frequency**: DAILY
- **dbt Tag**: `tag:stream_bcfinsg`

### Key Components

#### 🗂️ **Source Data**
```yaml
sources:
  - name: gdw1_raw
    database: ps_cld_rw
    schema: stg
    tables:
      - name: bcfinsg  # EBCDIC → Snowflake conversion
        description: "Plan Balance Segment Master data"
        columns:
          - BCF_ACCOUNT_NO1      # Account number (16 chars)
          - BCF_PLAN_ID          # Plan identifier (6 chars)
          - BCF_DT_CURR_PROC     # Current processing date
          - BCF_OPENING_BALANCE  # Opening balance (DECIMAL)
          # ... 50+ additional fields
```

#### 🔍 **Validation Layer** - `int_validate_bcfinsg.sql`

**Purpose**: Comprehensive data validation with error logging
- **Materialization**: `view` (errors only)
- **Error Destination**: Central `XFM_ERR_DTL` table
- **Validation Types**:

```sql
-- Header validation (via pre_hook)
validate_header('BCFINSG')
validate_single_open_business_date('BCFINSG')

-- Data field validation (in model)
WITH source_validation AS (
  SELECT *,
    -- Date field validation using UDF
    {{ dcf_database_ref() }}.fn_is_valid_dt(BCF_DT_FIRST_TRANS) as valid_first_trans,
    {{ dcf_database_ref() }}.fn_is_valid_dt(BCF_DT_LAST_PAYMENT) as valid_last_payment,
    -- ... validation for 17 date fields
    
    -- Overall record validation
    CASE WHEN (
      NOT valid_first_trans OR
      NOT valid_last_payment OR 
      -- ... other validations
    ) THEN TRUE ELSE FALSE END as has_error
  FROM {{ source('gdw1_raw', 'bcfinsg') }}
)
SELECT * FROM source_validation WHERE has_error = TRUE
```

**Error Record Structure**:
```sql
-- XFM_ERR_DTL columns populated
STRM_NM: 'BCFINSG'
PRCS_NM: 'BCFINSG_VALIDATION'  
ERR_C: 'DQ001'  -- Data Quality error code
ERR_MSG: 'Invalid date field: BCF_DT_FIRST_TRANS'
ERR_SRC_KEY_VAL: BCF_ACCOUNT_NO1
BUSINESS_DATE: Current business date
PRCS_INST: Process instance ID
```

#### 💰 **Target Layer** - `plan_baln_segm_mstr.sql`

**Purpose**: Final business table with Type 2 SCD
- **Materialization**: `ibrg_cld_table` (Iceberg cloud table)
- **Strategy**: `truncate-load`
- **Dependencies**: Validation must pass first

```sql
{{
  config(
    materialized='ibrg_cld_table',
    incremental_strategy='truncate-load',
    database=var('target_database'),
    schema='PDSRCCS',
    pre_hook=[
      "{{ validate_single_open_business_date('BCFINSG') }}",
      "{{ register_process_instance('BCFINSG_XFM_LOAD', 'BCFINSG') }}"
    ],
    post_hook=[
      "{{ mark_process_completed('BCFINSG_XFM_LOAD', 'BCFINSG') }}"
    ]
  )
}}
```

**Business Transformations**:
- ✅ **EBCDIC Date Conversion**: `YYYYMMDD` format validation and conversion
- ✅ **Data Type Casting**: COMP-3 decimals → Snowflake types
- ✅ **Field Trimming**: Remove EBCDIC padding
- ✅ **Audit Columns**: System timestamps and process tracking
- ✅ **SCD Type 2**: Historical preservation with effective/expiry dates

---

## 🛠️ Validation Framework

### Comprehensive Validation Architecture

![Comprehensive Validation Framework](../../../others/resources/validation.svg)

### 1. Header/Trailer Validation - `validate_header_trailer()`

**Purpose**: File structure and integrity validation
**Scope**: Both BCFINSG (EBCDIC) and CSEL4 (CSV) formats

```sql
-- Usage in pre_hook
pre_hook=[
  "{{ validate_header_trailer('CSEL4_CPL_BUS_APP', 
       var('staging_database') ~ '.' ~ var('staging_schema') ~ '.cse_cpl_bus_app') }}"
]
```

**Validation Checks**:
- **FVL0001**: Header record presence
- **FVL0002**: Processing date matches business date
- **FVL0003**: File name pattern matching (optional)
- **FVL0005**: Record count validation (trailer vs actual)
- **FVL0006**: Trailer record presence

**Error Handling**:
```sql
-- Automatic failure on validation issues
{% if not validation_passed %}
  {{ exceptions.raise_compiler_error("Header/Trailer Validation Failed: " ~ status_message) }}
{% endif %}
```

### 2. Business Date Validation - `validate_single_open_business_date()`

**Purpose**: Ensure stream is in valid running state
**Usage**: Pre-hook for all models

```sql
-- Validates exactly one open business date
SELECT count(*) as open_count
FROM DCF_T_STRM_BUS_DT
WHERE STRM_NAME = 'BCFINSG' AND PROCESSING_FLAG = 1

-- Fails if open_count != 1
{% if open_count == 0 %}
  {{ exceptions.raise_compiler_error("No open business dates - start stream first") }}
{% elif open_count > 1 %}
  {{ exceptions.raise_compiler_error("Multiple open business dates - resolve conflicts") }}
{% endif %}
```

### 3. Data Quality Validation - UDF Integration

**Purpose**: Field-level validation using Snowflake UDFs
**Pattern**: Reuse existing DataStage validation logic

```sql
-- Date validation UDF (mirrors DataStage logic)
{{ dcf_database_ref() }}.fn_is_valid_dt(date_field) as is_valid

-- Usage in validation models
CASE 
  WHEN NOT {{ dcf_database_ref() }}.fn_is_valid_dt(BCF_DT_FIRST_TRANS) 
  THEN 'DQ001: Invalid first transaction date'
  ELSE NULL
END as error_message
```

---

## 🎯 Model Layer Separation

### **3-Tier Architecture Pattern**

#### **Layer 1: Raw/Staging** (`sources.yml`)
- **Purpose**: External data source definitions
- **Technology**: Snowflake external tables, pipes, or manual loads
- **Scope**: Minimal transformations, schema enforcement
- **Naming**: `gdw1_raw.bcfinsg`, `gdw1_raw.cse_cpl_bus_app`

#### **Layer 2: Intermediate** (`models/*/intermediate/`)
- **Purpose**: Data validation, cleansing, business logic preparation
- **Materialization**: `view` (for validation models), `table` (for complex transforms)
- **Scope**: Error detection, data quality, dependency management
- **Naming Pattern**: `int_validate_*`, `int_tmp_*`

**Examples**:
```yaml
int_validate_bcfinsg.sql      # BCFINSG validation → XFM_ERR_DTL
int_validate_cse_cpl_bus_app.sql # CSEL4 validation → XFM_ERR_DTL  
int_tmp_appt_dept.sql         # CSEL4 department transform
int_tmp_appt_pdct.sql         # CSEL4 product transform
ref_map_cse_pack_pdct_pl.sql  # Reference/lookup table
```

#### **Layer 3: Marts** (`models/*/marts/`)
- **Purpose**: Final business tables ready for consumption
- **Materialization**: `ibrg_cld_table` (Iceberg cloud tables with SCD Type 2)
- **Scope**: Production-ready data with full audit trail
- **Naming Pattern**: Business entity names

**Examples**:
```yaml
plan_baln_segm_mstr.sql  # BCFINSG target table
appt_dept.sql            # CSEL4 appointment-department relationship
appt_pdct.sql            # CSEL4 appointment-product relationship  
dept_appt.sql            # CSEL4 department-appointment relationship
```

### **Tag-Based Organization**

**Stream Tags**:
```yaml
# Run entire BCFINSG stream
dbt run --select tag:stream_bcfinsg

# Run entire CSEL4 stream  
dbt run --select tag:stream_csel4
```

**Layer Tags**:
```yaml
# Run only validation layer
dbt run --select tag:intermediate_layer

# Run only target tables
dbt run --select tag:marts_layer

# Run specific process types
dbt run --select tag:data_validation
dbt run --select tag:core_transform
```

**Model-Specific Tags**:
```sql
-- Example: BCFINSG validation model
tags: [
  'stream_bcfinsg',           # Stream identifier
  'process_bcfinsg_validation', # Process identifier  
  'intermediate_layer',       # Architecture layer
  'data_validation',          # Functional category
  'error_detection'           # Technical capability
]
```

---

## 🔄 Hook Framework

### **Pre-Hooks** - Validation & Setup
**Purpose**: Ensure prerequisites before model execution

```sql
pre_hook=[
  "{{ validate_single_open_business_date('BCFINSG') }}",  # Stream state check
  "{{ validate_header('BCFINSG') }}",                     # File header validation
  "{{ register_process_instance('PROCESS_NAME', 'BCFINSG') }}", # DCF tracking
  "{{ err_tbl_reset('BCFINSG', 'PROCESS_NAME') }}"        # Error table cleanup
]
```

**Execution Order**: Pre-hooks run sequentially before model SQL

### **Post-Hooks** - Completion & Error Handling
**Purpose**: Finalize processing and handle results

```sql
post_hook=[
  "{{ load_errors_to_central_table(this, 'BCFINSG', 'PROCESS_NAME') }}", # Error logging
  "{{ check_error_and_end_prcs('BCFINSG', 'PROCESS_NAME') }}"            # Process completion
]
```

**Error Handling Logic**:
```sql
-- check_error_and_end_prcs() logic
{% if error_count > 0 %}
  -- Update process status to ERROR
  -- Log failure to DCF_T_EXEC_LOG
  {{ exceptions.raise_compiler_error("Process failed with " ~ error_count ~ " errors") }}
{% else %}
  -- Update process status to COMPLETED
  -- Log success to DCF_T_EXEC_LOG
{% endif %}
```

---

## 📊 DCF Integration

### **Stream Lifecycle Management**

#### **1. Stream Initialization**
```bash
# Start stream for specific business date
dbt run-operation start_stream_op --args '{
  stream_name: "BCFINSG", 
  business_date: "2024-12-20"
}'
```

**What it does**:
- Creates stream instance in `DCF_T_STRM_INST`
- Sets business date in `DCF_T_STRM_BUS_DT` with `PROCESSING_FLAG = 1`
- Initializes process tracking tables
- Validates stream configuration

#### **2. Stream Execution**
```bash
# Run all BCFINSG models using tags
dbt run --select tag:stream_bcfinsg
```

**Execution Flow**:
1. **Validation Layer**: `int_validate_bcfinsg` checks data quality
2. **Error Handling**: Errors logged to `XFM_ERR_DTL`, process fails if errors found
3. **Transform Layer**: `plan_baln_segm_mstr` processes clean data
4. **Audit Trail**: All steps logged to `DCF_T_EXEC_LOG`

#### **3. Stream Completion**
```bash
# End stream and mark complete
dbt run-operation end_stream_op --args '{stream_name: "BCFINSG"}'
```

**What it does**:
- Updates `DCF_T_STRM_BUS_DT` with `PROCESSING_FLAG = 2` (COMPLETED)
- Finalizes all process instances
- Creates completion audit records

### **Process Instance Tracking**

**Each model execution creates**:
```sql
-- DCF_T_PRCS_INST record
INSERT INTO DCF_T_PRCS_INST (
  PRCS_INST_ID,     -- Auto-generated sequence
  STRM_NAME,        -- 'BCFINSG' 
  PRCS_NAME,        -- 'BCFINSG_VALIDATION'
  PRCS_BUS_DT,      -- Current business date
  PRCS_STATUS,      -- 'RUNNING' → 'COMPLETED'/'ERROR'
  PRCS_START_TS,    -- Process start timestamp
  PRCS_END_TS       -- Process end timestamp
)
```

### **Execution Logging**

**Detailed audit trail**:
```sql
-- DCF_T_EXEC_LOG records for each step
INSERT INTO DCF_T_EXEC_LOG (
  STRM_ID,          -- Stream identifier
  PRCS_NAME,        -- Process name
  STEP_NAME,        -- 'HEADER_VALIDATION', 'DATA_TRANSFORM', etc.
  STEP_STATUS,      -- 'STARTED', 'COMPLETED', 'ERROR'
  MESSAGE_TYPE,     -- 'INFO', 'WARNING', 'ERROR'
  MESSAGE_TEXT,     -- Detailed execution message
  ERROR_CODE,       -- Standard error codes
  BUSINESS_DATE,    -- Processing business date
  CREATED_TS        -- Log entry timestamp
)
```

---

## ❌ Error Management System

### **Centralized Error Table** - `XFM_ERR_DTL`

**Purpose**: Single repository for all validation errors across streams

```sql
-- Error record structure
CREATE TABLE XFM_ERR_DTL (
  ERR_INST_ID       NUMBER(38,0),    -- Error instance ID
  STRM_NM           VARCHAR(50),     -- 'BCFINSG', 'CSEL4_CPL_BUS_APP'
  PRCS_NM           VARCHAR(100),    -- Process name
  ERR_C             VARCHAR(20),     -- Error code (DQ001, FVL0002, etc.)
  ERR_MSG           VARCHAR(4000),   -- Error description
  ERR_SRC_KEY_VAL   VARCHAR(500),    -- Source record key for debugging
  BUSINESS_DATE     DATE,            -- Processing date
  PRCS_INST         NUMBER(38,0),    -- Process instance ID
  CREATED_TS        TIMESTAMP_NTZ    -- Error detection timestamp
)
```

### **Error Loading Pattern**

**All validation models follow this pattern**:

```sql
-- 1. Model materializes as VIEW with only error records
{{ config(materialized='view') }}

WITH validation_results AS (
  SELECT *, 
    CASE WHEN validation_failed THEN TRUE ELSE FALSE END as has_error
  FROM source_data
)
SELECT * FROM validation_results WHERE has_error = TRUE

-- 2. Post-hook loads errors to central table
post_hook=[
  "{{ load_errors_to_central_table(this, 'BCFINSG', 'BCFINSG_VALIDATION') }}"
]
```

**Macro Implementation**:
```sql
{% macro load_errors_to_central_table(source_view, stream_name, process_name) %}
  INSERT INTO {{ var('dcf_database') }}.{{ var('dcf_schema') }}.XFM_ERR_DTL 
  SELECT * FROM {{ source_view }}
  WHERE STRM_NM = '{{ stream_name }}' AND PRCS_NM = '{{ process_name }}'
{% endmacro %}
```

### **Error Recovery Process**

**1. Error Detection**:
```bash
# Check for errors in current run
snow sql -c pupad_svc -q "
SELECT ERR_C, ERR_MSG, COUNT(*) as error_count
FROM PSUND_MIGR_DCF.P_D_DCF_001_STD_0.XFM_ERR_DTL 
WHERE STRM_NM = 'BCFINSG' AND BUSINESS_DATE = CURRENT_DATE
GROUP BY ERR_C, ERR_MSG
ORDER BY error_count DESC"
```

**2. Error Analysis**:
```bash
# Get detailed error records with source keys
snow sql -c pupad_svc -q "
SELECT ERR_SRC_KEY_VAL, ERR_MSG, CREATED_TS
FROM PSUND_MIGR_DCF.P_D_DCF_001_STD_0.XFM_ERR_DTL 
WHERE STRM_NM = 'BCFINSG' AND ERR_C = 'DQ001'
ORDER BY CREATED_TS DESC
LIMIT 10"
```

**3. Error Resolution**:
- Fix source data issues
- Clear error table for reprocessing: `DELETE FROM XFM_ERR_DTL WHERE STRM_NM = 'BCFINSG' AND BUSINESS_DATE = 'YYYY-MM-DD'`
- Restart stream: `dbt run --select tag:stream_bcfinsg`

---

## 🚀 Operations Guide

### **Daily Operations Workflow**

#### **1. Pre-Flight Checks**
```bash
# Check stream status
dbt run-operation get_stream_status_op --args '{stream_name: "BCFINSG"}'

# Verify DCF infrastructure
snow sql -c pupad_svc -q "SELECT * FROM PSUND_MIGR_DCF.P_D_DCF_001_STD_0.V_DCF_STREAM_STATUS"
```

#### **2. Start Streams**
```bash
# Start BCFINSG for today
dbt run-operation start_stream_op --args '{
  stream_name: "BCFINSG", 
  business_date: "2024-12-20"
}'

# Start CSEL4 for today
dbt run-operation start_stream_op --args '{
  stream_name: "CSEL4", 
  business_date: "2024-12-20"
}'
```

#### **3. Execute Processing**
```bash
# Process BCFINSG stream (validation → transform → load)
dbt run --select tag:stream_bcfinsg

# Process CSEL4 stream (validation → intermediate → marts)
dbt run --select tag:stream_csel4
```

#### **4. Monitor & Validate**
```bash
# Check for processing errors
dbt test --select tag:stream_bcfinsg
dbt test --select tag:stream_csel4

# Verify target table counts
snow sql -c pupad_svc -q "SELECT COUNT(*) FROM ps_cld_rw.PDSRCCS.PLAN_BALN_SEGM_MSTR WHERE EFFT_D = '2024-12-20'"
```

#### **5. Complete Streams**
```bash
# Mark streams as complete
dbt run-operation end_stream_op --args '{stream_name: "BCFINSG"}'
dbt run-operation end_stream_op --args '{stream_name: "CSEL4"}'
```

### **Performance Metrics**

**Typical Execution Times**:
- **BCFINSG Stream**: ~30 seconds end-to-end
  - Validation: 10-15 seconds
  - Transform/Load: 15-20 seconds
- **CSEL4 Stream**: ~45 seconds end-to-end  
  - Validation: 15-20 seconds
  - Intermediate: 10-15 seconds
  - Target Tables: 15-20 seconds

**Scalability**:
- **Concurrency**: 4 threads (configurable)
- **Warehouse**: Auto-scaling for load spikes
- **Incremental**: Type 2 SCD for historical preservation

---

## 📈 Key Benefits

### **vs DataStage Implementation**

| **Aspect** | **DataStage (Legacy)** | **dbt + DCF (Modern)** |
|------------|------------------------|-------------------------|
| **Orchestration** | 6 SequenceJobs + complex dependencies | Single tag-based execution |
| **Error Handling** | Separate error jobs per stream | Centralized XFM_ERR_DTL table |
| **Code Maintenance** | GUI-based, version control challenges | Git-based, peer review, CI/CD |
| **Testing** | Manual validation procedures | Automated dbt tests + data quality |
| **Monitoring** | Custom monitoring solutions | Built-in DCF audit framework |
| **Documentation** | Separate documentation systems | Self-documenting models + lineage |
| **Deployment** | Complex environment promotion | Simple `dbt deploy` workflow |

### **Enterprise Features**

✅ **Comprehensive Audit Trail**: Every execution logged to DCF tables  
✅ **Data Lineage**: Automatic lineage tracking via dbt  
✅ **Error Recovery**: Centralized error management and reprocessing  
✅ **Type 2 SCD**: Historical preservation with Iceberg tables  
✅ **Stream Management**: Business date management and stream lifecycle  
✅ **Data Quality**: Built-in validation with custom UDFs  
✅ **Scalability**: Cloud-native scaling with Snowflake  
✅ **DevOps Ready**: Git integration, CI/CD, automated testing  

---

## 🔧 Development Guide

### **Adding New Streams**

**1. Register Stream in DCF**:
```sql
-- Add to schema_definitions/dcf_schema_init.sql
INSERT INTO DCF_T_STRM (STRM_ID, STRM_NAME, STRM_TYPE, BUSINESS_DOMAIN, TARGET_SCHEMA)
VALUES (1492, 'NEW_STREAM', 'DAILY', 'OPERATIONS', 'TARGET_SCHEMA');
```

**2. Create Model Structure**:
```bash
mkdir -p models/new_stream/{intermediate,marts,reference}
```

**3. Implement Validation Model**:
```sql
-- models/new_stream/intermediate/int_validate_new_stream.sql
{{ config(
  materialized='view',
  pre_hook=["{{ validate_single_open_business_date('NEW_STREAM') }}"],
  post_hook=["{{ load_errors_to_central_table(this, 'NEW_STREAM', 'NEW_STREAM_VALIDATION') }}"],
  tags=['stream_new_stream', 'data_validation']
) }}
```

**4. Add Stream Tags**:
```yaml
# dbt_project.yml
models:
  gdw1_csel4_migration:
    new_stream:
      +tags: ["stream_new_stream"]
```

### **Custom Validation Rules**

**Create UDF for complex validation**:
```sql
-- Example: Custom business rule validation
CREATE OR REPLACE FUNCTION validate_business_rule(field1 STRING, field2 NUMBER)
RETURNS BOOLEAN
LANGUAGE SQL
AS $$
  CASE 
    WHEN field1 IS NULL OR field2 <= 0 THEN FALSE
    WHEN field1 NOT RLIKE '^[A-Z]{3}[0-9]{3}$' THEN FALSE
    ELSE TRUE
  END
$$;
```

**Use in validation model**:
```sql
CASE 
  WHEN NOT validate_business_rule(FIELD1, FIELD2) 
  THEN 'BR001: Business rule violation'
  ELSE NULL
END as error_message
```

---

## 🧪 Development and Testing

### **Testing Individual Models**

For development and testing of specific models, use the following workflow:

#### **1. Validate dbt Configuration**
```bash
# Navigate to the dbt project directory
cd /Users/psundaram/Documents/prjs/cba/agentic/bteq_agentic/dcf

# Check dbt version and plugins
dbt --version

# Validate connection and configuration
dbt debug
```

**Expected output:**
```
06:57:33  Connection test: [OK connection ok]
06:57:33  All checks passed!
```

#### **2. List Available Models**
```bash
# List all models in the project
dbt ls
```

This will show the full model path, e.g.:
```
gdw1_csel4_migration.acct_baln.bkdt_adj.acct_baln_bkdt_adj_rule
```

#### **3. Run Specific Models**
```bash
# Run a specific model by name
dbt run -s acct_baln_bkdt_adj_rule

# Run models by tag
dbt run --select tag:account_balance
dbt run --select tag:core_transform
```

**Example successful execution:**
```
06:58:18  Concurrency: 4 threads (target='dev')
06:58:22  1 of 1 START sql ibrg_cld_table model ps_cld_rw.pddstg.acct_baln_bkdt_adj_rule . [RUN]
06:58:31  1 of 1 OK created sql ibrg_cld_table model ps_cld_rw.pddstg.acct_baln_bkdt_adj_rule  [SUCCESS 0 in 8.99s]
06:58:33  Completed successfully
06:58:33  Done. PASS=1 WARN=0 ERROR=0 SKIP=0 NO-OP=0 TOTAL=1
```

#### **4. Model Development Tips**

**Syntax Validation:**
- All models use Snowflake-specific SQL syntax
- Date functions: Use `DATEADD()`, `DATEDIFF()`, `DATE_TRUNC()`
- Avoid PostgreSQL/Oracle syntax like `INTERVAL` or `MONTHS_BETWEEN()`

**Custom Materialization:**
- Models use `ibrg_cld_table` materialization strategy
- Includes automatic process logging via pre/post hooks
- Supports incremental strategy 'truncate-load'

**Error Troubleshooting:**
```bash
# Check compilation without running
dbt compile -s model_name

# Parse project for syntax errors  
dbt parse

# View compiled SQL
cat target/compiled/gdw1_csel4_migration/models/path/to/model.sql
```

#### **5. Testing Framework Integration**

The models integrate with DCF's comprehensive testing framework:

```bash
# Run data tests
dbt test -s model_name

# Run with verbose output for debugging
dbt run -s model_name --verbose

# Check dbt logs
tail -f logs/dbt.log
```

**✅ Model Validation Checklist:**
- [ ] Model compiles without syntax errors
- [ ] Executes successfully against Snowflake
- [ ] Uses correct Snowflake date/time functions  
- [ ] Includes proper DCF logging hooks
- [ ] Follows naming conventions
- [ ] Includes appropriate tags for stream management

---

## 📚 References

- **DCF Framework Documentation**: `gdw2_igsn_xfm/transformation_frmwk/docu/`
- **BCFINSG DataStage Analysis**: `_docs/curr_state/BCFINSG/`
- **Target State Design**: `_docs/target_state/BCFINSG/`
- **Operations Demo**: `OPERATIONS_DEMO.md`
- **dbt Documentation**: [docs.getdbt.com](https://docs.getdbt.com)
- **Snowflake dbt Guide**: [docs.snowflake.com/en/user-guide/ecosystem-dbt](https://docs.snowflake.com/en/user-guide/ecosystem-dbt)

---

**🎯 The GDW1 dbt Migration Framework provides enterprise-grade ETL capabilities with modern cloud-native architecture, comprehensive validation, and operational excellence!** 🚀

---

## 📊 Complete Architecture Diagram - Both Streams

The following diagram shows the complete architecture with both CCODS and CSEL4 streams side by side:

![Complete GDW1 Architecture - Both Streams](../../../others/resources/complete_archi.svg)

This comprehensive diagram shows how both streams integrate with the shared DCF Control Framework and error management system, making it easy to understand the complete GDW1 migration architecture.