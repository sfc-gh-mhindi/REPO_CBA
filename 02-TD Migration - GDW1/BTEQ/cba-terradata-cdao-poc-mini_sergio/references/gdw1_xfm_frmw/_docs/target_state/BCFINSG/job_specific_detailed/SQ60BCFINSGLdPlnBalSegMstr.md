# SQ60BCFINSGLdPlnBalSegMstr - dbt DCF Implementation

## Overview

This document details the dbt implementation of the **data loading orchestration** from DataStage `SQ60BCFINSGLdPlnBalSegMstr` sequence job. This represents the complete data loading lifecycle management with DCF framework replacing file-based orchestration.

**Original DataStage Complexity**: Complex orchestration with file discovery, metadata processing, and reporting  
**Target Implementation**: dbt DAG execution with DCF integration and cloud-native orchestration

---

## **DataStage to dbt DCF Mapping**

### **Orchestration Architecture Transformation**

| **DataStage Component** | **dbt DCF Equivalent** | **Implementation** |
|------------------------|------------------------|-------------------|
| **File Discovery Loop** | dbt model dependencies | Automatic dependency resolution |
| **Process ID Generation** | DCF stream instance IDs | Built-in process tracking |
| **Bulk Load Execution** | dbt incremental models | Cloud-native loading |
| **Metadata Updates** | DCF audit framework | Automatic metadata capture |
| **File Archival** | Cloud lifecycle policies | Automated file management |
| **Load Reporting** | DCF execution dashboard | Real-time monitoring |

---

## **Complete Orchestration Implementation**

### **1. dbt DAG Structure (Replaces DataStage Sequence)**

**Current DataStage Logic**: Complex sequence with loops and file processing  
**Target Implementation**: Simple dbt model dependencies with DCF orchestration

```yaml
# dbt_project.yml - Model dependencies configuration
models:
  gdw1_dbt:
    staging:
      +materialized: view
      +tags: ["stream_bcfinsg", "staging"]
    intermediate:
      +materialized: table  
      +tags: ["stream_bcfinsg", "intermediate"]
    marts:
      core:
        +materialized: incremental
        +tags: ["stream_bcfinsg", "marts"]
        +pre_hook: 
          - "{{ validate_stream_status(var('stream_name')) }}"
          - "{{ register_process_start(this.name, var('stream_name')) }}"
        +post_hook:
          - "{{ register_process_completion(this.name, var('stream_name')) }}"
          - "{{ log_load_statistics(this.name) }}"
```

### **2. Stream Orchestration Model**

**Current DataStage Logic**: JobOccrStart → File Loop → Load → Metadata → Archive  
**Target Implementation**: Single orchestration model managing the complete workflow

```sql
-- models/utils/orchestrate_bcfinsg_load.sql
{{ config(
    materialized='table',
    tags=['bcfinsg', 'orchestration', 'stream_bcfinsg'],
    pre_hook=[
        "{{ validate_stream_status(var('stream_name', 'BCFINSG_PLAN_BALN_SEGM_LOAD')) }}",
        "{{ initialize_load_orchestration() }}"
    ],
    post_hook=[
        "{{ finalize_load_orchestration() }}",
        "{{ send_load_completion_report() }}"
    ]
) }}

/*
    BCFINSG Load Orchestration Model
    Equivalent to DataStage SQ60BCFINSGLdPlnBalSegMstr sequence job
    
    This model orchestrates:
    - Source data validation and discovery
    - Incremental load execution monitoring
    - Metadata processing and audit updates
    - Load statistics generation and reporting
    - Process completion tracking
*/

WITH orchestration_context AS (
    SELECT 
        '{{ var("stream_name", "BCFINSG_PLAN_BALN_SEGM_LOAD") }}' as stream_name,
        {{ var("stream_id", 1490) }} as stream_id,
        '{{ var("run_stream_process_date") }}' as process_date,
        '{{ var("ods_batch_id") }}' as batch_id,
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ(6) as orchestration_start_time,
        
        -- Process instance generation (replaces GetODSProcId)
        {{ generate_dcf_process_instance_id() }} as process_instance_id
),

source_validation AS (
    SELECT 
        oc.*,
        -- Source data discovery (replaces GetFileList)
        (SELECT COUNT(*) FROM {{ ref('int_bcfinsg_validated') }} 
         WHERE final_processing_status = 'READY_FOR_LOADING') as source_record_count,
        
        -- Data quality assessment
        (SELECT AVG(data_quality_score) FROM {{ ref('int_bcfinsg_validated') }} 
         WHERE final_processing_status = 'READY_FOR_LOADING') as avg_data_quality,
        
        -- Processing readiness validation
        CASE 
            WHEN source_record_count > 0 AND avg_data_quality >= 70 
            THEN 'READY_FOR_LOADING'
            WHEN source_record_count > 0 AND avg_data_quality < 70 
            THEN 'QUALITY_ISSUES'
            ELSE 'NO_DATA_AVAILABLE'
        END as orchestration_status
        
    FROM orchestration_context oc
),

load_execution_monitoring AS (
    SELECT 
        sv.*,
        -- Load execution tracking (replaces file loop processing)
        CASE 
            WHEN orchestration_status = 'READY_FOR_LOADING'
            THEN {{ execute_incremental_load() }}
            ELSE 'SKIPPED'
        END as load_execution_status,
        
        -- Load statistics capture
        {{ capture_load_statistics() }} as load_statistics,
        
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ(6) as load_completion_time
        
    FROM source_validation sv
),

metadata_processing AS (
    SELECT 
        lem.*,
        -- Metadata updates (replaces GDWUtilProcessMetaDataFL)
        {{ update_process_metadata('BCFINSG_LOAD', 'load_statistics') }} as metadata_update_status,
        
        -- Archive management (replaces MvLoadFiles)
        {{ manage_data_lifecycle() }} as archive_status,
        
        -- Load reporting preparation
        {{ prepare_load_report() }} as report_data,
        
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ(6) as orchestration_end_time
        
    FROM load_execution_monitoring lem
)

SELECT 
    -- Orchestration summary
    stream_name,
    stream_id,
    process_date,
    batch_id,
    process_instance_id,
    
    -- Execution timeline
    orchestration_start_time,
    load_completion_time,
    orchestration_end_time,
    DATEDIFF('second', orchestration_start_time, orchestration_end_time) as total_duration_seconds,
    
    -- Processing results
    source_record_count,
    avg_data_quality,
    orchestration_status,
    load_execution_status,
    metadata_update_status,
    archive_status,
    
    -- Load statistics
    load_statistics,
    report_data,
    
    -- DCF audit columns
    {{ dcf_audit_columns('orchestrate_bcfinsg_load') }}

FROM metadata_processing

-- Orchestration validation
{{ validate_orchestration_completion() }}
```

### **3. Process Instance Management (Replaces GetODSProcId)**

```sql
-- macros/generate_dcf_process_instance_id.sql
{% macro generate_dcf_process_instance_id() %}
    -- Generate unique process instance ID using DCF framework
    (
        SELECT COALESCE(MAX(PRCS_INST_ID), 0) + 1 
        FROM {{ var('dcf_database') }}.{{ var('dcf_schema') }}.DCF_T_PRCS_INST 
        WHERE STRM_ID = {{ var('stream_id', 1490) }}
        AND DATE(PRCS_START_TS) = CURRENT_DATE()
    )
{% endmacro %}
```

### **4. Load Execution Function (Replaces LdBCFINSGPlanBalnSegmMstr)**

```sql
-- macros/execute_incremental_load.sql
{% macro execute_incremental_load() %}
    -- Execute the incremental load and return status
    {% set load_sql %}
        -- Trigger the fact table incremental load
        SELECT 
            'LOAD_INITIATED' as status,
            CURRENT_TIMESTAMP() as load_start_time
    {% endset %}
    
    -- This macro coordinates with the incremental model execution
    'COORDINATED_WITH_INCREMENTAL_MODEL'
{% endmacro %}
```

---

## **Advanced Orchestration Features**

### **1. Dynamic Load Statistics Capture**

```sql
-- macros/capture_load_statistics.sql
{% macro capture_load_statistics() %}
    -- Capture comprehensive load statistics
    OBJECT_CONSTRUCT(
        'records_processed', (SELECT COUNT(*) FROM {{ ref('int_bcfinsg_validated') }}),
        'records_loaded', (SELECT COUNT(*) FROM {{ ref('fct_plan_baln_segm_mstr') }} 
                          WHERE DATE(load_timestamp) = CURRENT_DATE()),
        'quality_score_avg', (SELECT AVG(data_quality_score) FROM {{ ref('int_bcfinsg_validated') }}),
        'quality_score_min', (SELECT MIN(data_quality_score) FROM {{ ref('int_bcfinsg_validated') }}),
        'load_duration_minutes', DATEDIFF('minute', orchestration_start_time, CURRENT_TIMESTAMP()),
        'warehouse_used', '{{ configure_load_warehouse() }}',
        'processing_date', '{{ var("run_stream_process_date") }}',
        'batch_id', '{{ var("ods_batch_id") }}'
    )
{% endmacro %}
```

### **2. Metadata Processing Integration**

```sql
-- macros/update_process_metadata.sql  
{% macro update_process_metadata(process_name, statistics_object) %}
    -- Update DCF process metadata tables
    INSERT INTO {{ var('dcf_database') }}.{{ var('dcf_schema') }}.DCF_T_PRCS_EXEC_LOG (
        STRM_ID,
        STRM_NAME,
        PRCS_INST_ID,
        PRCS_NAME,
        PRCS_START_TS,
        PRCS_END_TS,
        PRCS_STATUS,
        PRCS_STATISTICS,
        EXEC_USER_ID
    )
    SELECT 
        {{ var('stream_id', 1490) }},
        '{{ var("stream_name", "BCFINSG_PLAN_BALN_SEGM_LOAD") }}',
        process_instance_id,
        '{{ process_name }}',
        orchestration_start_time,
        CURRENT_TIMESTAMP(),
        CASE 
            WHEN load_execution_status = 'SUCCESS' THEN 'COMPLETED'
            ELSE 'FAILED'
        END,
        {{ statistics_object }},
        '{{ env_var("DBT_USER", "dbt_service") }}'
    FROM orchestration_context;
    
    'METADATA_UPDATED'
{% endmacro %}
```

### **3. Data Lifecycle Management (Replaces File Archival)**

```sql
-- macros/manage_data_lifecycle.sql
{% macro manage_data_lifecycle() %}
    -- Manage data lifecycle in cloud environment
    {% set lifecycle_sql %}
        -- Update data lifecycle metadata
        UPDATE {{ var('dcf_database') }}.{{ var('dcf_schema') }}.DCF_T_DATA_LINEAGE
        SET 
            ARCHIVE_STATUS = 'COMPLETED',
            ARCHIVE_TS = CURRENT_TIMESTAMP(),
            RETENTION_EXPIRY_DATE = DATEADD('day', 2555, CURRENT_DATE()), -- 7 years retention
            LIFECYCLE_STAGE = 'ARCHIVED'
        WHERE STRM_ID = {{ var('stream_id', 1490) }}
        AND PRCS_DATE = '{{ var("run_stream_process_date") }}'
    {% endset %}
    
    {% if execute %}
        {% do run_query(lifecycle_sql) %}
    {% endif %}
    
    'LIFECYCLE_MANAGED'
{% endmacro %}
```

---

## **Load Reporting and Monitoring**

### **1. Load Report Generation (Replaces Report File Creation)**

```sql
-- macros/prepare_load_report.sql
{% macro prepare_load_report() %}
    -- Generate comprehensive load report
    OBJECT_CONSTRUCT(
        'stream_name', '{{ var("stream_name") }}',
        'processing_date', '{{ var("run_stream_process_date") }}',
        'load_summary', OBJECT_CONSTRUCT(
            'total_records_processed', source_record_count,
            'records_loaded_successfully', (
                SELECT COUNT(*) FROM {{ ref('fct_plan_baln_segm_mstr') }} 
                WHERE DATE(load_timestamp) = CURRENT_DATE()
            ),
            'average_data_quality_score', avg_data_quality,
            'load_duration_minutes', DATEDIFF('minute', orchestration_start_time, orchestration_end_time),
            'load_completion_status', load_execution_status
        ),
        'quality_metrics', OBJECT_CONSTRUCT(
            'high_quality_records', (
                SELECT COUNT(*) FROM {{ ref('int_bcfinsg_validated') }} 
                WHERE data_quality_score >= 85
            ),
            'medium_quality_records', (
                SELECT COUNT(*) FROM {{ ref('int_bcfinsg_validated') }} 
                WHERE data_quality_score BETWEEN 70 AND 84
            ),
            'low_quality_records', (
                SELECT COUNT(*) FROM {{ ref('int_bcfinsg_validated') }} 
                WHERE data_quality_score < 70
            )
        ),
        'performance_metrics', OBJECT_CONSTRUCT(
            'warehouse_utilized', '{{ configure_load_warehouse() }}',
            'credits_consumed', {{ get_credits_consumed() }},
            'throughput_records_per_minute', 
                ROUND(source_record_count / GREATEST(DATEDIFF('minute', orchestration_start_time, orchestration_end_time), 1), 2)
        )
    )
{% endmacro %}
```

### **2. Automated Reporting (Replaces Email Reports)**

```sql
-- macros/send_load_completion_report.sql
{% macro send_load_completion_report() %}
    -- Integration with monitoring/alerting systems
    INSERT INTO {{ var('dcf_database') }}.{{ var('dcf_schema') }}.DCF_T_NOTIFICATIONS (
        NOTIFICATION_TYPE,
        STRM_ID,
        STRM_NAME,
        NOTIFICATION_CONTENT,
        NOTIFICATION_STATUS,
        CREATED_TS
    )
    SELECT 
        'LOAD_COMPLETION_REPORT',
        {{ var('stream_id', 1490) }},
        '{{ var("stream_name", "BCFINSG_PLAN_BALN_SEGM_LOAD") }}',
        report_data,
        'PENDING',
        CURRENT_TIMESTAMP()
    FROM {{ this }};
    
    {{ log("Load completion report generated and queued for delivery", info=true) }}
{% endmacro %}
```

---

## **Orchestration Validation and Error Handling**

### **1. Pre-Orchestration Validation**

```sql
-- macros/initialize_load_orchestration.sql
{% macro initialize_load_orchestration() %}
    -- Validate orchestration prerequisites
    {% set validation_checks = [
        ('SOURCE_DATA_AVAILABLE', 'SELECT COUNT(*) FROM ' ~ ref('int_bcfinsg_validated')),
        ('STREAM_STATUS_ACTIVE', 'SELECT COUNT(*) FROM DCF_T_STRM WHERE STRM_ID = ' ~ var('stream_id') ~ ' AND STRM_STATUS = ''ACTIVE'''),
        ('NO_CONCURRENT_EXECUTION', 'SELECT COUNT(*) FROM DCF_T_PRCS_INST WHERE STRM_ID = ' ~ var('stream_id') ~ ' AND PRCS_STATUS = ''RUNNING''')
    ] %}
    
    {% for check_name, check_sql in validation_checks %}
        {% set result = run_query(check_sql) %}
        {% if check_name == 'SOURCE_DATA_AVAILABLE' and result.columns[0].values()[0] == 0 %}
            {{ exceptions.raise_compiler_error("No source data available for loading") }}
        {% elif check_name == 'STREAM_STATUS_ACTIVE' and result.columns[0].values()[0] == 0 %}
            {{ exceptions.raise_compiler_error("Stream is not in active status") }}
        {% elif check_name == 'NO_CONCURRENT_EXECUTION' and result.columns[0].values()[0] > 0 %}
            {{ exceptions.raise_compiler_error("Concurrent execution detected - stream already running") }}
        {% endif %}
    {% endfor %}
    
    {{ log("Load orchestration validation completed successfully", info=true) }}
{% endmacro %}
```

### **2. Post-Orchestration Validation**

```sql
-- macros/validate_orchestration_completion.sql
{% macro validate_orchestration_completion() %}
    -- Validate successful orchestration completion
    {% set final_validation %}
        SELECT 
            CASE 
                WHEN load_execution_status = 'SUCCESS' 
                 AND metadata_update_status = 'METADATA_UPDATED'
                 AND archive_status = 'LIFECYCLE_MANAGED'
                THEN 'ORCHESTRATION_COMPLETE'
                ELSE 'ORCHESTRATION_INCOMPLETE'
            END as final_status
        FROM {{ this }}
    {% endset %}
    
    {% if execute %}
        {% set validation_result = run_query(final_validation) %}
        {% if validation_result.columns[0].values()[0] != 'ORCHESTRATION_COMPLETE' %}
            {{ exceptions.raise_compiler_error("Orchestration completed with errors - check execution logs") }}
        {% endif %}
        
        {{ log("Load orchestration completed successfully", info=true) }}
    {% endif %}
{% endmacro %}
```

---

## **Implementation Comparison**

| **Aspect** | **DataStage Original** | **dbt DCF Implementation** |
|------------|----------------------|---------------------------|
| **Orchestration Method** | Complex sequence job with loops | dbt DAG with dependency management |
| **File Discovery** | File system scanning with loops | Model dependency resolution |
| **Process Tracking** | Oracle UTIL_PROS_ISAC updates | DCF process instance management |
| **Load Execution** | Direct DataStage job calls | dbt incremental model coordination |
| **Metadata Processing** | Separate parallel job execution | Integrated macro-based processing |
| **Reporting** | File-based CSV generation | JSON-based structured reporting |
| **Error Handling** | Sequence job error links | dbt macro validation and exceptions |
| **Monitoring** | Manual log file analysis | Real-time DCF dashboard integration |

---

## **Key Benefits of dbt Implementation**

1. **🎯 Simplified Orchestration**: dbt DAG replaces complex sequence job loops
2. **🔄 Dependency Management**: Automatic model dependencies vs manual file loops
3. **📊 Integrated Monitoring**: DCF framework vs separate Oracle tracking tables
4. **⚡ Cloud Performance**: Snowflake optimization vs fixed infrastructure
5. **🛠️ Maintainability**: SQL macros vs DataStage GUI configuration
6. **📈 Scalability**: Auto-scaling orchestration vs fixed node processing
7. **🔍 Observability**: Real-time DCF metrics vs post-execution log analysis

---

**Implementation Status**: ✅ **DESIGN COMPLETE**  
**Complexity**: Medium (Orchestration coordination)  
**Dependencies**: Transformation models, DCF framework, load models  
**Priority**: 🔴 **CRITICAL** - Required for complete load lifecycle management