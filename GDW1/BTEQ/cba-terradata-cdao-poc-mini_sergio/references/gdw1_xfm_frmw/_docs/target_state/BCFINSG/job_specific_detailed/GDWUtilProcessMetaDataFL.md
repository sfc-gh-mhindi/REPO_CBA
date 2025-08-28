# GDWUtilProcessMetaDataFL - dbt DCF Implementation

## Overview

This document details the dbt implementation of the **metadata and audit tracking** from DataStage `GDWUtilProcessMetaDataFL` job. This functionality is now **built into the DCF framework** with automatic metadata capture, eliminating the need for separate metadata processing jobs.

**Original DataStage Complexity**: FastLoad log parsing and manual metadata updates  
**Target Implementation**: Automatic DCF audit framework with real-time metadata capture

---

## **DataStage to dbt DCF Mapping**

### **Metadata Processing Architecture Transformation**

| **DataStage Component** | **dbt DCF Equivalent** | **Implementation** |
|------------------------|------------------------|-------------------|
| **FastLoad Log Reader** | DCF execution metrics | Automatic capture during model execution |
| **Row Count Extraction** | dbt compilation stats | Built-in record counting |
| **UTIL_PROS_ISAC Updates** | DCF_T_PRCS_INST updates | Automatic process instance tracking |
| **Load Statistics Reports** | DCF dashboard metrics | Real-time operational dashboard |
| **Manual Log Parsing** | Structured JSON metrics | Automatic metrics collection |

---

## **Critical Insight: Built-in DCF Functionality**

### **🔄 No Separate Model Required**

**Why DCF Eliminates This Component:**
- **Automatic Metadata Capture**: Every dbt model execution automatically captures metadata
- **Real-time Statistics**: No need for post-processing log file parsing
- **Built-in Audit Trail**: DCF framework maintains complete execution history
- **Integrated Reporting**: Operational dashboards replace manual report generation

### **DCF Framework Integration Points**

```sql
-- Every dbt model automatically includes DCF metadata capture
{{ config(
    post_hook=[
        "{{ log_model_execution_stats(this.name) }}", -- Replaces GDWUtilProcessMetaDataFL
        "{{ update_process_metadata_dcf(this.name) }}" -- Automatic metadata updates
    ]
) }}
```

---

## **Automatic Metadata Implementation**

### **1. DCF Execution Statistics (Replaces Log Parsing)**

**Current DataStage Logic**: Parse FastLoad logs to extract row counts  
**Target Implementation**: Automatic capture during model execution

```sql
-- macros/log_model_execution_stats.sql
{% macro log_model_execution_stats(model_name) %}
    -- Automatic metadata capture for every model execution
    INSERT INTO {{ var('dcf_database') }}.{{ var('dcf_schema') }}.DCF_T_EXEC_LOG (
        STRM_ID,
        STRM_NAME,
        PRCS_NAME,
        MODEL_NAME,
        EXEC_START_TS,
        EXEC_END_TS,
        EXEC_STATUS,
        RECORDS_PROCESSED,
        RECORDS_OUTPUT,
        PROCESSING_DURATION_SECONDS,
        WAREHOUSE_USED,
        QUERY_ID,
        CREDITS_CONSUMED,
        BYTES_PROCESSED,
        COMPILATION_TIME_SECONDS,
        EXECUTION_STATISTICS
    )
    SELECT 
        {{ var('stream_id', 1490) }},
        '{{ var("stream_name", "BCFINSG_PLAN_BALN_SEGM_LOAD") }}',
        SPLIT_PART('{{ model_name }}', '.', -1),
        '{{ model_name }}',
        '{{ run_started_at }}',
        CURRENT_TIMESTAMP(),
        'SUCCESS',
        -- Automatic record counting (replaces log parsing)
        (SELECT COUNT(*) FROM {{ ref('int_bcfinsg_validated') }}),
        (SELECT COUNT(*) FROM {{ this }}),
        DATEDIFF('second', '{{ run_started_at }}', CURRENT_TIMESTAMP()),
        CURRENT_WAREHOUSE(),
        LAST_QUERY_ID(),
        {{ get_query_credits_consumed() }},
        {{ get_bytes_processed() }},
        {{ get_compilation_time() }},
        -- Structured execution statistics (replaces manual log analysis)
        OBJECT_CONSTRUCT(
            'model_materialization', '{{ config.get("materialized", "table") }}',
            'incremental_strategy', '{{ config.get("incremental_strategy", "N/A") }}',
            'data_quality_avg', (SELECT AVG(data_quality_score) FROM {{ this }}),
            'transformation_errors', (SELECT COUNT(*) FROM {{ this }} WHERE transformation_error_type != 'VALID'),
            'load_pattern', '{{ "INCREMENTAL" if is_incremental() else "FULL" }}',
            'dcf_audit_complete', true
        )
{% endmacro %}
```

### **2. Process Instance Updates (Replaces UTIL_PROS_ISAC)**

**Current DataStage Logic**: Manual updates to Oracle control tables  
**Target Implementation**: Automatic DCF process instance management

```sql
-- macros/update_process_metadata_dcf.sql
{% macro update_process_metadata_dcf(model_name) %}
    -- Automatic process instance updates (replaces UTIL_PROS_ISAC functionality)
    UPDATE {{ var('dcf_database') }}.{{ var('dcf_schema') }}.DCF_T_PRCS_INST 
    SET 
        PRCS_END_TS = CURRENT_TIMESTAMP(),
        PRCS_STATUS = 'COMPLETED',
        RECORDS_PROCESSED = (SELECT COUNT(*) FROM {{ this }}),
        PROCESSING_DURATION = DATEDIFF('second', PRCS_START_TS, CURRENT_TIMESTAMP()),
        PRCS_STATISTICS = OBJECT_CONSTRUCT(
            'model_name', '{{ model_name }}',
            'execution_method', 'DBT_MODEL',
            'warehouse_used', CURRENT_WAREHOUSE(),
            'query_complexity', '{{ get_query_complexity() }}',
            'data_freshness_hours', {{ calculate_data_freshness() }},
            'quality_score_distribution', {{ get_quality_distribution() }}
        ),
        UPD_TS = CURRENT_TIMESTAMP(),
        UPD_USER_ID = '{{ env_var("DBT_USER", "dbt_service") }}'
    WHERE STRM_ID = {{ var('stream_id', 1490) }}
    AND PRCS_START_TS::DATE = CURRENT_DATE()
    AND PRCS_STATUS = 'RUNNING';
    
    {{ log("Process metadata updated automatically via DCF framework", info=true) }}
{% endmacro %}
```

### **3. Load Statistics Dashboard (Replaces Report Files)**

**Current DataStage Logic**: Generate CSV reports for manual analysis  
**Target Implementation**: Real-time operational dashboard with DCF metrics

```sql
-- models/utils/util_bcfinsg_operational_dashboard.sql
{{ config(
    materialized='view',
    tags=['bcfinsg', 'monitoring', 'dashboard']
) }}

/*
    Real-time Operational Dashboard for BCFINSG Processing
    Replaces manual report generation from GDWUtilProcessMetaDataFL
    
    Provides real-time visibility into:
    - Processing performance and throughput
    - Data quality metrics and trends
    - Load statistics and completion status
    - Error rates and issue identification
    - Resource utilization and cost tracking
*/

WITH current_execution AS (
    SELECT 
        STRM_NAME,
        PRCS_NAME,
        MAX(EXEC_END_TS) as latest_execution,
        COUNT(*) as total_executions_today,
        AVG(PROCESSING_DURATION_SECONDS) as avg_duration_seconds,
        SUM(RECORDS_PROCESSED) as total_records_processed,
        SUM(CREDITS_CONSUMED) as total_credits_consumed
    FROM {{ var('dcf_database') }}.{{ var('dcf_schema') }}.DCF_T_EXEC_LOG
    WHERE STRM_ID = {{ var('stream_id', 1490) }}
    AND DATE(EXEC_START_TS) = CURRENT_DATE()
    AND EXEC_STATUS = 'SUCCESS'
    GROUP BY STRM_NAME, PRCS_NAME
),

quality_metrics AS (
    SELECT 
        AVG(data_quality_score) as current_avg_quality,
        MIN(data_quality_score) as current_min_quality,
        MAX(data_quality_score) as current_max_quality,
        COUNT(CASE WHEN data_quality_score >= 85 THEN 1 END) as high_quality_count,
        COUNT(CASE WHEN data_quality_score < 70 THEN 1 END) as low_quality_count,
        COUNT(*) as total_record_count
    FROM {{ ref('int_bcfinsg_validated') }}
    WHERE DATE(transformation_timestamp) = CURRENT_DATE()
),

performance_metrics AS (
    SELECT 
        MAX(load_timestamp) as latest_load_time,
        COUNT(*) as records_loaded_today,
        DATEDIFF('hour', MIN(load_timestamp), MAX(load_timestamp)) as processing_window_hours,
        COUNT(*) / GREATEST(DATEDIFF('hour', MIN(load_timestamp), MAX(load_timestamp)), 1) as avg_throughput_per_hour
    FROM {{ ref('fct_plan_baln_segm_mstr') }}
    WHERE DATE(load_timestamp) = CURRENT_DATE()
)

SELECT 
    -- Processing Summary
    '{{ var("stream_name", "BCFINSG_PLAN_BALN_SEGM_LOAD") }}' as stream_name,
    CURRENT_DATE() as processing_date,
    CURRENT_TIMESTAMP() as dashboard_refresh_time,
    
    -- Execution Statistics (replaces FastLoad log analysis)
    ce.latest_execution,
    ce.total_executions_today,
    ce.avg_duration_seconds,
    ROUND(ce.avg_duration_seconds / 60, 2) as avg_duration_minutes,
    ce.total_records_processed,
    ce.total_credits_consumed,
    
    -- Data Quality Metrics
    qm.current_avg_quality,
    qm.current_min_quality,
    qm.current_max_quality,
    qm.high_quality_count,
    qm.low_quality_count,
    qm.total_record_count,
    ROUND((qm.high_quality_count::FLOAT / qm.total_record_count) * 100, 2) as high_quality_percentage,
    
    -- Performance Metrics
    pm.latest_load_time,
    pm.records_loaded_today,
    pm.processing_window_hours,
    pm.avg_throughput_per_hour,
    
    -- Operational Status
    CASE 
        WHEN ce.latest_execution > CURRENT_TIMESTAMP() - INTERVAL '2 hours' THEN 'CURRENT'
        WHEN ce.latest_execution > CURRENT_TIMESTAMP() - INTERVAL '6 hours' THEN 'RECENT'
        ELSE 'STALE'
    END as data_freshness_status,
    
    CASE 
        WHEN qm.current_avg_quality >= 85 THEN 'EXCELLENT'
        WHEN qm.current_avg_quality >= 75 THEN 'GOOD'  
        WHEN qm.current_avg_quality >= 60 THEN 'ACCEPTABLE'
        ELSE 'NEEDS_ATTENTION'
    END as quality_status,
    
    CASE 
        WHEN pm.avg_throughput_per_hour >= 100000 THEN 'HIGH_PERFORMANCE'
        WHEN pm.avg_throughput_per_hour >= 50000 THEN 'GOOD_PERFORMANCE'
        WHEN pm.avg_throughput_per_hour >= 10000 THEN 'ACCEPTABLE_PERFORMANCE'
        ELSE 'LOW_PERFORMANCE'
    END as performance_status

FROM current_execution ce
CROSS JOIN quality_metrics qm  
CROSS JOIN performance_metrics pm
```

---

## **Advanced DCF Metadata Features**

### **1. Automated Data Lineage Tracking**

```sql
-- macros/track_data_lineage.sql
{% macro track_data_lineage(model_name) %}
    -- Automatic data lineage capture (replaces manual metadata tracking)
    INSERT INTO {{ var('dcf_database') }}.{{ var('dcf_schema') }}.DCF_T_DATA_LINEAGE (
        STRM_ID,
        SOURCE_MODEL,
        TARGET_MODEL,
        TRANSFORMATION_TYPE,
        PROCESSING_DATE,
        RECORD_COUNT_SOURCE,
        RECORD_COUNT_TARGET,
        DATA_QUALITY_IMPACT,
        LINEAGE_METADATA
    )
    SELECT 
        {{ var('stream_id', 1490) }},
        '{{ ref('stg_bcfinsg_plan_baln_segm').name }}',
        '{{ model_name }}',
        'DBT_TRANSFORMATION',
        CURRENT_DATE(),
        (SELECT COUNT(*) FROM {{ ref('stg_bcfinsg_plan_baln_segm') }}),
        (SELECT COUNT(*) FROM {{ this }}),
        (SELECT AVG(data_quality_score) FROM {{ this }}),
        OBJECT_CONSTRUCT(
            'transformation_rules_applied', {{ get_transformation_rules() }},
            'business_logic_version', '{{ var("business_logic_version", "1.0") }}',
            'dbt_model_version', '{{ model.version }}',
            'data_lineage_complete', true
        )
{% endmacro %}
```

### **2. Real-time Quality Monitoring**

```sql
-- macros/monitor_data_quality.sql
{% macro monitor_data_quality() %}
    -- Real-time quality monitoring (replaces post-processing analysis)
    {% set quality_thresholds = {
        'avg_quality_threshold': 75,
        'min_quality_threshold': 50,
        'error_rate_threshold': 5
    } %}
    
    {% set quality_check %}
        SELECT 
            AVG(data_quality_score) as current_avg_quality,
            MIN(data_quality_score) as current_min_quality,
            (COUNT(CASE WHEN transformation_error_type != 'VALID' THEN 1 END) * 100.0 / COUNT(*)) as error_rate_percentage
        FROM {{ this }}
    {% endset %}
    
    {% if execute %}
        {% set quality_result = run_query(quality_check) %}
        {% for row in quality_result %}
            {% if row[0] < quality_thresholds['avg_quality_threshold'] %}
                {{ log("WARNING: Average data quality (" ~ row[0] ~ ") below threshold (" ~ quality_thresholds['avg_quality_threshold'] ~ ")", info=true) }}
            {% endif %}
            {% if row[1] < quality_thresholds['min_quality_threshold'] %}
                {{ log("ALERT: Minimum data quality (" ~ row[1] ~ ") below threshold (" ~ quality_thresholds['min_quality_threshold'] ~ ")", info=true) }}
            {% endif %}
            {% if row[2] > quality_thresholds['error_rate_threshold'] %}
                {{ log("WARNING: Error rate (" ~ row[2] ~ "%) above threshold (" ~ quality_thresholds['error_rate_threshold'] ~ "%)", info=true) }}
            {% endif %}
        {% endfor %}
    {% endif %}
{% endmacro %}
```

---

## **Historical Reporting and Analytics**

### **1. Historical Performance Trends**

```sql
-- models/utils/util_bcfinsg_performance_trends.sql
{{ config(
    materialized='view',
    tags=['bcfinsg', 'analytics', 'trends']
) }}

/*
    Historical Performance Trends Analysis
    Replaces manual historical reporting from GDWUtilProcessMetaDataFL
*/

SELECT 
    DATE(EXEC_START_TS) as processing_date,
    COUNT(*) as total_executions,
    AVG(PROCESSING_DURATION_SECONDS) as avg_duration_seconds,
    SUM(RECORDS_PROCESSED) as total_records_processed,
    SUM(CREDITS_CONSUMED) as total_credits_consumed,
    AVG(JSON_EXTRACT_PATH_TEXT(EXECUTION_STATISTICS, 'data_quality_avg')) as avg_quality_score,
    
    -- Performance trending
    LAG(AVG(PROCESSING_DURATION_SECONDS)) OVER (ORDER BY DATE(EXEC_START_TS)) as prev_avg_duration,
    LAG(SUM(RECORDS_PROCESSED)) OVER (ORDER BY DATE(EXEC_START_TS)) as prev_records_processed,
    
    -- Performance indicators
    CASE 
        WHEN AVG(PROCESSING_DURATION_SECONDS) <= LAG(AVG(PROCESSING_DURATION_SECONDS)) OVER (ORDER BY DATE(EXEC_START_TS)) 
        THEN 'IMPROVING'
        ELSE 'DEGRADING'
    END as performance_trend,
    
    CASE 
        WHEN SUM(RECORDS_PROCESSED) >= LAG(SUM(RECORDS_PROCESSED)) OVER (ORDER BY DATE(EXEC_START_TS))
        THEN 'STABLE_OR_GROWING'
        ELSE 'DECLINING'
    END as volume_trend

FROM {{ var('dcf_database') }}.{{ var('dcf_schema') }}.DCF_T_EXEC_LOG
WHERE STRM_ID = {{ var('stream_id', 1490) }}
AND EXEC_STATUS = 'SUCCESS'
AND DATE(EXEC_START_TS) >= CURRENT_DATE() - 30  -- Last 30 days
GROUP BY DATE(EXEC_START_TS)
ORDER BY processing_date DESC
```

---

## **Implementation Comparison**

| **Aspect** | **DataStage Original** | **dbt DCF Implementation** |
|------------|----------------------|---------------------------|
| **Metadata Capture** | Manual FastLoad log parsing | Automatic DCF framework capture |
| **Statistics Collection** | Post-processing batch job | Real-time execution metrics |
| **Reporting** | CSV file generation | Live operational dashboard |
| **Data Quality Monitoring** | Manual log analysis | Real-time quality alerts |
| **Performance Tracking** | Historical log review | Continuous performance monitoring |
| **Error Detection** | Batch error analysis | Real-time error alerting |
| **Maintenance** | Separate job scheduling | Built-in framework functionality |
| **Scalability** | Fixed processing capacity | Auto-scaling cloud infrastructure |

---

## **Key Benefits of DCF Implementation**

1. **🔄 Automatic Operation**: No separate metadata job - built into every model execution
2. **⚡ Real-time Metrics**: Live dashboard vs post-processing batch reports
3. **📊 Comprehensive Tracking**: Structured JSON metrics vs manual log parsing
4. **🎯 Integrated Monitoring**: Single framework vs multiple separate systems
5. **🛠️ Zero Maintenance**: Built-in functionality vs custom job management
6. **📈 Advanced Analytics**: Historical trends and predictive insights
7. **🔍 Quality Assurance**: Real-time quality monitoring vs batch validation

---

## **Migration Notes**

### **✅ What Gets Replaced**
- **FastLoad Log Parsing**: Automatic execution metrics
- **UTIL_PROS_ISAC Updates**: DCF process instance management
- **Manual Report Generation**: Real-time operational dashboard
- **Row Count Validation**: Built-in record counting
- **Performance Analysis**: Continuous performance monitoring

### **🔄 What Gets Enhanced**
- **Real-time Monitoring**: Live operational dashboard
- **Structured Metrics**: JSON-based vs text-based logs
- **Historical Analytics**: Trend analysis and predictive insights
- **Quality Assurance**: Continuous quality monitoring
- **Cost Tracking**: Credit consumption and resource utilization

---

**Implementation Status**: ✅ **BUILT-IN DCF FUNCTIONALITY**  
**Complexity**: Low (Framework provides all functionality)  
**Dependencies**: DCF framework configuration  
**Priority**: 🟡 **IMPORTANT** - Monitoring and operational visibility