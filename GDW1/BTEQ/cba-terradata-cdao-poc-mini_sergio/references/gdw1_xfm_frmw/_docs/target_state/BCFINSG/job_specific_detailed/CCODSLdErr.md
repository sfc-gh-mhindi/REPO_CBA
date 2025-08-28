# CCODSLdErr - dbt DCF Implementation

## Overview

This document details the dbt implementation of the **error data loading** from DataStage `CCODSLdErr` parallel job. This functionality is **built into the DCF framework** with automatic error record capture and centralized error repository management.

**Original DataStage Complexity**: Parallel job for error record loading to centralized table  
**Target Implementation**: Automatic DCF error logging with structured error management

---

## **DataStage to dbt DCF Mapping**

### **Error Loading Architecture Transformation**

| **DataStage Component** | **dbt DCF Equivalent** | **Implementation** |
|------------------------|------------------------|-------------------|
| **Error File Reader** | Model error capture hooks | Real-time error detection |
| **UTIL_TRSF_EROR_RQM3 Loader** | DCF_T_ERROR_LOG table | Centralized DCF error repository |
| **Error Record Structure** | Structured JSON error metadata | Enhanced error information |
| **Parallel Loading** | Automatic error processing | Real-time error capture |

---

## **Critical Insight: Built-in DCF Error Repository**

### **🔄 No Separate Loading Job Required**

**Why DCF Eliminates This Component:**
- **Automatic Error Loading**: Every dbt model execution automatically loads errors to DCF repository
- **Real-time Error Capture**: No need for separate error file processing
- **Structured Error Storage**: Enhanced error metadata vs flat file structure
- **Centralized Error Management**: Single DCF error framework vs multiple error tables

### **DCF Error Repository Integration**

```sql
-- Automatic error loading with every model validation
WITH error_detection AS (
    SELECT *,
        -- Error detection logic (replaces error file reading)
        CASE 
            WHEN transformation_error_type != 'VALID' THEN true
            ELSE false
        END as has_error
    FROM {{ ref('int_bcfinsg_validated') }}
),

error_loading AS (
    -- Automatic error loading (replaces CCODSLdErr functionality)  
    INSERT INTO {{ var('dcf_database') }}.{{ var('dcf_schema') }}.DCF_T_ERROR_LOG (
        -- Enhanced error structure vs original UTIL_TRSF_EROR_RQM3
        error_details...
    )
    SELECT error_data FROM error_detection WHERE has_error
)
```

---

## **Enhanced Error Repository Implementation**

### **1. Automatic Error Record Loading (Replaces CCODSLdErr)**

**Current DataStage Logic**: Separate parallel job to load error files  
**Target Implementation**: Real-time error capture integrated into model execution

```sql
-- macros/capture_error_records.sql
{% macro capture_error_records(model_name) %}
    -- Automatic error record loading (replaces CCODSLdErr parallel job)
    
    {% set error_loading_sql %}
        INSERT INTO {{ var('dcf_database') }}.{{ var('dcf_schema') }}.DCF_T_ERROR_LOG (
            STRM_ID,
            STRM_NAME,
            PRCS_NAME,
            MODEL_NAME,
            ERROR_RECORD_ID,
            SOURCE_KEY_ID,
            ERROR_TYPE_CODE,
            TRANSFORMATION_COLUMN,
            VALUE_BEFORE_TRANSFORMATION,
            VALUE_AFTER_TRANSFORMATION,
            SOURCE_FILE_NAME,
            SOURCE_ROW_NUMBER,
            TRANSFORMATION_JOB_NAME,
            ERROR_TIMESTAMP,
            ERROR_SEVERITY,
            ERROR_CATEGORY,
            BUSINESS_IMPACT,
            ERROR_METADATA,
            REMEDIATION_STATUS,
            CREATED_TS,
            CREATED_USER_ID
        )
        SELECT 
            {{ var('stream_id', 1490) }},
            '{{ var("stream_name", "BCFINSG_PLAN_BALN_SEGM_LOAD") }}',
            SPLIT_PART('{{ model_name }}', '.', -1),
            '{{ model_name }}',
            -- Generate unique error record ID
            {{ dbt_utils.generate_surrogate_key(['business_key', 'transformation_error_type', 'transformation_timestamp']) }},
            
            -- Source identification (enhanced vs original)
            business_key as source_key_id,
            transformation_error_type as error_type_code,
            
            -- Enhanced transformation context
            CASE transformation_error_type
                WHEN 'MISSING_ACCOUNT_NUMBER' THEN 'account_number'
                WHEN 'INVALID_ACCOUNT_FORMAT' THEN 'account_number'
                WHEN 'MISSING_PLAN_CODE' THEN 'plan_code'
                WHEN 'INVALID_DATE_FORMAT' THEN 'effective_date'
                WHEN 'BUSINESS_RULE_VIOLATION' THEN 'business_logic'
                ELSE 'general_validation'
            END as transformation_column,
            
            -- Value tracking (before/after transformation)
            COALESCE(
                CASE transformation_error_type
                    WHEN 'MISSING_ACCOUNT_NUMBER' THEN account_number
                    WHEN 'INVALID_ACCOUNT_FORMAT' THEN account_number
                    WHEN 'MISSING_PLAN_CODE' THEN plan_code
                    ELSE 'N/A'
                END, ''
            ) as value_before_transformation,
            
            'VALIDATION_FAILED' as value_after_transformation,
            
            -- Source context
            source_file_name,
            source_row_number,
            '{{ model_name }}' as transformation_job_name,
            transformation_timestamp as error_timestamp,
            
            -- Enhanced error classification
            CASE 
                WHEN data_quality_score = 0 THEN 'CRITICAL'
                WHEN data_quality_score < 50 THEN 'HIGH'
                WHEN data_quality_score < 70 THEN 'MEDIUM'
                ELSE 'LOW'
            END as error_severity,
            
            -- Error categorization for analysis
            CASE transformation_error_type
                WHEN 'MISSING_ACCOUNT_NUMBER' THEN 'DATA_COMPLETENESS'
                WHEN 'INVALID_ACCOUNT_FORMAT' THEN 'DATA_FORMAT'
                WHEN 'MISSING_PLAN_CODE' THEN 'DATA_COMPLETENESS'
                WHEN 'INVALID_DATE_FORMAT' THEN 'DATE_VALIDATION'
                WHEN 'BUSINESS_RULE_VIOLATION' THEN 'BUSINESS_LOGIC'
                ELSE 'DATA_QUALITY'
            END as error_category,
            
            -- Business impact assessment
            CASE 
                WHEN transformation_error_type IN ('MISSING_ACCOUNT_NUMBER', 'MISSING_PLAN_CODE') THEN 'HIGH'
                WHEN transformation_error_type LIKE '%INVALID%' THEN 'MEDIUM'
                ELSE 'LOW'
            END as business_impact,
            
            -- Comprehensive error metadata (enhanced vs original flat structure)
            OBJECT_CONSTRUCT(
                'original_record', OBJECT_CONSTRUCT(
                    'account_number', account_number,
                    'plan_code', plan_code,
                    'segment_code', segment_code,
                    'effective_date', effective_date,
                    'balance_amount', balance_amount
                ),
                'validation_context', OBJECT_CONSTRUCT(
                    'validation_status', validation_status,
                    'business_validation_status', business_validation_status,
                    'final_validation_status', final_validation_status,
                    'data_quality_score', data_quality_score
                ),
                'processing_context', OBJECT_CONSTRUCT(
                    'batch_id', batch_id,
                    'run_stream_process_date', run_stream_process_date,
                    'load_timestamp', load_timestamp,
                    'staging_record_key', staging_record_key
                ),
                'error_analysis', OBJECT_CONSTRUCT(
                    'error_detection_method', 'DBT_VALIDATION',
                    'error_capture_timestamp', CURRENT_TIMESTAMP(),
                    'model_execution_context', '{{ model_name }}',
                    'dcf_integration', true
                )
            ) as error_metadata,
            
            'LOGGED' as remediation_status,
            CURRENT_TIMESTAMP() as created_ts,
            '{{ env_var("DBT_USER", "dbt_service") }}' as created_user_id
            
        FROM {{ this }}
        WHERE transformation_error_type != 'VALID'
        AND DATE(transformation_timestamp) = CURRENT_DATE()
    {% endset %}
    
    {% if execute %}
        {% do run_query(error_loading_sql) %}
        {% set error_count_sql %}
            SELECT COUNT(*) FROM {{ this }}
            WHERE transformation_error_type != 'VALID'
        {% endset %}
        {% set error_count_result = run_query(error_count_sql) %}
        {% set error_count = error_count_result.columns[0].values()[0] %}
        
        {{ log("Automatically loaded " ~ error_count ~ " error records to DCF error repository for " ~ model_name, info=true) }}
    {% endif %}
{% endmacro %}
```

### **2. Enhanced Error Repository Structure**

**Current DataStage Logic**: Basic UTIL_TRSF_EROR_RQM3 table structure  
**Target Implementation**: Enhanced DCF error repository with rich metadata

```sql
-- Enhanced error repository structure (vs original UTIL_TRSF_EROR_RQM3)
CREATE TABLE IF NOT EXISTS {{ var('dcf_database') }}.{{ var('dcf_schema') }}.DCF_T_ERROR_LOG (
    -- Core identification (enhanced vs original)
    ERROR_LOG_ID NUMBER IDENTITY(1,1) PRIMARY KEY,
    STRM_ID NUMBER NOT NULL,
    STRM_NAME VARCHAR(100) NOT NULL,
    PRCS_NAME VARCHAR(100) NOT NULL,
    MODEL_NAME VARCHAR(200) NOT NULL,
    ERROR_RECORD_ID VARCHAR(500) NOT NULL,
    
    -- Source tracking (equivalent to original)
    SOURCE_KEY_ID VARCHAR(500),
    ERROR_TYPE_CODE VARCHAR(100) NOT NULL,
    TRANSFORMATION_COLUMN VARCHAR(100),
    VALUE_BEFORE_TRANSFORMATION VARCHAR(1000),
    VALUE_AFTER_TRANSFORMATION VARCHAR(1000),
    SOURCE_FILE_NAME VARCHAR(500),
    SOURCE_ROW_NUMBER NUMBER,
    TRANSFORMATION_JOB_NAME VARCHAR(200),
    
    -- Enhanced error classification (new capabilities)
    ERROR_TIMESTAMP TIMESTAMP_NTZ(6) NOT NULL,
    ERROR_SEVERITY VARCHAR(20) NOT NULL, -- CRITICAL, HIGH, MEDIUM, LOW
    ERROR_CATEGORY VARCHAR(50) NOT NULL, -- DATA_COMPLETENESS, DATA_FORMAT, etc.
    BUSINESS_IMPACT VARCHAR(20) NOT NULL, -- HIGH, MEDIUM, LOW
    REMEDIATION_STATUS VARCHAR(50) DEFAULT 'LOGGED', -- LOGGED, IN_PROGRESS, RESOLVED
    
    -- Rich metadata (major enhancement vs flat original)
    ERROR_METADATA OBJECT,
    
    -- Audit columns
    CREATED_TS TIMESTAMP_NTZ(6) DEFAULT CURRENT_TIMESTAMP(),
    CREATED_USER_ID VARCHAR(100) DEFAULT USER,
    UPDATED_TS TIMESTAMP_NTZ(6) DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_USER_ID VARCHAR(100) DEFAULT USER,
    
    -- Indexing for performance
    INDEX IX_ERROR_LOG_STRM_DATE (STRM_ID, ERROR_TIMESTAMP),
    INDEX IX_ERROR_LOG_TYPE_SEV (ERROR_TYPE_CODE, ERROR_SEVERITY),
    INDEX IX_ERROR_LOG_SOURCE (SOURCE_KEY_ID, SOURCE_FILE_NAME)
);
```

### **3. Error Repository Analytics and Reporting**

**Current DataStage Logic**: Basic error loading with minimal analysis  
**Target Implementation**: Advanced error analytics with trend analysis

```sql
-- models/utils/util_error_repository_analytics.sql
{{ config(
    materialized='view',
    tags=['bcfinsg', 'error_analytics', 'repository']
) }}

/*
    Comprehensive Error Repository Analytics
    Replaces basic error loading from CCODSLdErr with advanced analytics
*/

WITH error_summary AS (
    SELECT 
        DATE(error_timestamp) as error_date,
        error_type_code,
        error_severity,
        error_category,
        business_impact,
        COUNT(*) as error_count,
        COUNT(DISTINCT source_key_id) as unique_sources_affected,
        COUNT(DISTINCT source_file_name) as unique_files_affected,
        AVG(CAST(JSON_EXTRACT_PATH_TEXT(error_metadata, 'validation_context.data_quality_score') AS NUMBER)) as avg_quality_score
    FROM {{ var('dcf_database') }}.{{ var('dcf_schema') }}.DCF_T_ERROR_LOG
    WHERE strm_id = {{ var('stream_id', 1490) }}
    AND error_timestamp >= CURRENT_DATE() - 30
    GROUP BY 1,2,3,4,5
),

error_trends AS (
    SELECT 
        error_type_code,
        error_date,
        error_count,
        LAG(error_count) OVER (PARTITION BY error_type_code ORDER BY error_date) as prev_day_count,
        SUM(error_count) OVER (PARTITION BY error_type_code ORDER BY error_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as weekly_total
    FROM error_summary
),

error_hotspots AS (
    SELECT 
        source_file_name,
        error_type_code,
        COUNT(*) as error_frequency,
        COUNT(DISTINCT DATE(error_timestamp)) as days_with_errors,
        MIN(error_timestamp) as first_error,
        MAX(error_timestamp) as latest_error
    FROM {{ var('dcf_database') }}.{{ var('dcf_schema') }}.DCF_T_ERROR_LOG
    WHERE strm_id = {{ var('stream_id', 1490) }}
    AND error_timestamp >= CURRENT_DATE() - 7
    GROUP BY source_file_name, error_type_code
    HAVING COUNT(*) > 5 -- Only show frequent errors
)

SELECT 
    -- Error summary metrics
    es.error_date,
    es.error_type_code,
    es.error_severity,
    es.error_category,
    es.business_impact,
    es.error_count,
    es.unique_sources_affected,
    es.unique_files_affected,
    es.avg_quality_score,
    
    -- Trend analysis
    et.prev_day_count,
    et.weekly_total,
    CASE 
        WHEN es.error_count > et.prev_day_count THEN 'INCREASING'
        WHEN es.error_count < et.prev_day_count THEN 'DECREASING'
        ELSE 'STABLE'
    END as daily_trend,
    
    -- Impact assessment
    CASE 
        WHEN es.business_impact = 'HIGH' AND es.error_count > 10 THEN 'CRITICAL_IMPACT'
        WHEN es.business_impact = 'MEDIUM' AND es.error_count > 50 THEN 'SIGNIFICANT_IMPACT'
        WHEN es.error_count > 100 THEN 'HIGH_VOLUME'
        ELSE 'NORMAL'
    END as impact_level,
    
    -- Action recommendations
    CASE 
        WHEN es.error_category = 'DATA_COMPLETENESS' AND es.error_count > 20 
        THEN 'Review source data quality checks'
        WHEN es.error_category = 'DATA_FORMAT' AND et.weekly_total > 100 
        THEN 'Investigate data format validation rules'
        WHEN es.error_category = 'BUSINESS_LOGIC' 
        THEN 'Review business rule implementation'
        ELSE 'Monitor error patterns'
    END as recommended_action

FROM error_summary es
LEFT JOIN error_trends et ON es.error_date = et.error_date AND es.error_type_code = et.error_type_code
ORDER BY es.error_date DESC, es.error_count DESC
```

---

## **Advanced Error Management Features**

### **1. Error Pattern Detection**

```sql
-- macros/detect_error_patterns.sql
{% macro detect_error_patterns() %}
    -- Advanced error pattern detection (major enhancement vs original)
    
    {% set pattern_analysis %}
        WITH error_patterns AS (
            SELECT 
                error_type_code,
                source_file_name,
                DATE(error_timestamp) as error_date,
                COUNT(*) as daily_count,
                -- Pattern detection
                CASE 
                    WHEN COUNT(*) > 100 THEN 'HIGH_VOLUME_PATTERN'
                    WHEN COUNT(DISTINCT source_key_id) = COUNT(*) THEN 'WIDESPREAD_PATTERN'
                    WHEN COUNT(DISTINCT source_key_id) < COUNT(*) * 0.1 THEN 'CONCENTRATED_PATTERN'
                    ELSE 'NORMAL_PATTERN'
                END as error_pattern,
                
                -- Severity escalation
                CASE 
                    WHEN COUNT(*) > LAG(COUNT(*)) OVER (PARTITION BY error_type_code ORDER BY DATE(error_timestamp)) * 2
                    THEN 'ESCALATING'
                    ELSE 'STABLE'
                END as pattern_trend
                
            FROM {{ var('dcf_database') }}.{{ var('dcf_schema') }}.DCF_T_ERROR_LOG
            WHERE strm_id = {{ var('stream_id', 1490) }}
            AND error_timestamp >= CURRENT_DATE() - 7
            GROUP BY error_type_code, source_file_name, DATE(error_timestamp)
        )
        
        SELECT 
            error_type_code,
            error_pattern,
            pattern_trend,
            COUNT(*) as pattern_frequency
        FROM error_patterns
        WHERE error_pattern != 'NORMAL_PATTERN' OR pattern_trend = 'ESCALATING'
        GROUP BY error_type_code, error_pattern, pattern_trend
    {% endset %}
    
    {% if execute %}
        {% set pattern_result = run_query(pattern_analysis) %}
        {% for row in pattern_result %}
            {{ log("Error pattern detected: " ~ row[0] ~ " - " ~ row[1] ~ " (" ~ row[2] ~ ") - Frequency: " ~ row[3], info=true) }}
        {% endfor %}
    {% endif %}
{% endmacro %}
```

### **2. Error Remediation Tracking**

```sql
-- macros/track_error_remediation.sql  
{% macro track_error_remediation(model_name) %}
    -- Error remediation tracking and status management
    
    {% set remediation_update %}
        -- Update remediation status for resolved errors
        UPDATE {{ var('dcf_database') }}.{{ var('dcf_schema') }}.DCF_T_ERROR_LOG
        SET 
            remediation_status = 'RESOLVED',
            updated_ts = CURRENT_TIMESTAMP(),
            updated_user_id = '{{ env_var("DBT_USER", "dbt_service") }}',
            error_metadata = OBJECT_INSERT(
                error_metadata, 
                'remediation_info', 
                OBJECT_CONSTRUCT(
                    'resolution_method', 'AUTOMATIC_REPROCESSING',
                    'resolution_timestamp', CURRENT_TIMESTAMP(),
                    'resolution_model', '{{ model_name }}',
                    'resolution_success', true
                )
            )
        WHERE strm_id = {{ var('stream_id', 1490) }}
        AND error_type_code IN (
            -- Identify error types that were resolved in current execution
            SELECT DISTINCT previous_error_type
            FROM (
                SELECT 
                    business_key,
                    LAG(transformation_error_type) OVER (PARTITION BY business_key ORDER BY transformation_timestamp) as previous_error_type,
                    transformation_error_type as current_error_type
                FROM {{ this }}
            )
            WHERE previous_error_type != 'VALID' 
            AND current_error_type = 'VALID'
        )
        AND remediation_status = 'LOGGED'
        AND DATE(error_timestamp) >= CURRENT_DATE() - 7
    {% endset %}
    
    {% if execute %}
        {% do run_query(remediation_update) %}
        {{ log("Error remediation tracking updated for " ~ model_name, info=true) }}
    {% endif %}
{% endmacro %}
```

---

## **Error Repository Monitoring and Alerting**

### **1. Error Volume Monitoring**

```sql
-- macros/monitor_error_volume.sql
{% macro monitor_error_volume() %}
    -- Real-time error volume monitoring with alerting
    
    {% set volume_thresholds = {
        'daily_error_threshold': 1000,
        'critical_error_threshold': 50,
        'error_rate_threshold': 10
    } %}
    
    {% set volume_analysis %}
        SELECT 
            COUNT(*) as total_errors_today,
            COUNT(CASE WHEN error_severity = 'CRITICAL' THEN 1 END) as critical_errors_today,
            ROUND((COUNT(*) * 100.0 / (
                SELECT COUNT(*) FROM {{ ref('int_bcfinsg_validated') }}
                WHERE DATE(transformation_timestamp) = CURRENT_DATE()
            )), 2) as error_rate_percentage
        FROM {{ var('dcf_database') }}.{{ var('dcf_schema') }}.DCF_T_ERROR_LOG
        WHERE strm_id = {{ var('stream_id', 1490) }}
        AND DATE(error_timestamp) = CURRENT_DATE()
    {% endset %}
    
    {% if execute %}
        {% set volume_result = run_query(volume_analysis) %}
        {% for row in volume_result %}
            {% if row[0] > volume_thresholds['daily_error_threshold'] %}
                {{ log("🚨 HIGH ERROR VOLUME: " ~ row[0] ~ " errors today exceeds threshold (" ~ volume_thresholds['daily_error_threshold'] ~ ")", info=true) }}
            {% endif %}
            {% if row[1] > volume_thresholds['critical_error_threshold'] %}
                {{ log("🔴 CRITICAL ERRORS: " ~ row[1] ~ " critical errors today exceeds threshold (" ~ volume_thresholds['critical_error_threshold'] ~ ")", info=true) }}
            {% endif %}
            {% if row[2] > volume_thresholds['error_rate_threshold'] %}
                {{ log("📊 HIGH ERROR RATE: " ~ row[2] ~ "% error rate exceeds threshold (" ~ volume_thresholds['error_rate_threshold'] ~ "%)", info=true) }}
            {% endif %}
        {% endfor %}
    {% endif %}
{% endmacro %}
```

---

## **Implementation Comparison**

| **Aspect** | **DataStage Original** | **dbt DCF Implementation** |
|------------|----------------------|---------------------------|
| **Error Loading Method** | Separate parallel job | Real-time model integration |
| **Error File Processing** | Sequential file reading | Direct model error capture |
| **Error Table Structure** | Basic UTIL_TRSF_EROR_RQM3 | Enhanced DCF_T_ERROR_LOG |
| **Error Metadata** | Flat column structure | Rich JSON metadata |
| **Error Analysis** | Manual query analysis | Automated error analytics |
| **Error Tracking** | Basic error logging | Comprehensive lifecycle tracking |
| **Pattern Detection** | Manual investigation | Automated pattern recognition |
| **Remediation Tracking** | No tracking | Automated resolution monitoring |

---

## **Key Benefits of DCF Implementation**

1. **🔄 Real-time Loading**: Automatic error capture vs separate batch loading
2. **📊 Enhanced Metadata**: Rich JSON structure vs flat table columns
3. **🎯 Advanced Analytics**: Automated error analysis vs manual queries
4. **⚡ Pattern Detection**: Real-time pattern recognition vs manual investigation
5. **🛠️ Lifecycle Tracking**: Complete error lifecycle vs basic logging
6. **📈 Predictive Insights**: Trend analysis and error forecasting
7. **🔍 Root Cause Analysis**: Automated error classification and investigation

---

## **Migration Notes**

### **✅ What Gets Replaced**
- **Error File Reading**: Direct model error capture
- **UTIL_TRSF_EROR_RQM3 Loading**: Enhanced DCF_T_ERROR_LOG repository
- **Basic Error Structure**: Rich metadata with JSON structure
- **Manual Error Processing**: Automated error lifecycle management

### **🔄 What Gets Enhanced**
- **Real-time Error Loading**: Live error capture vs batch processing
- **Comprehensive Error Metadata**: Structured analysis vs flat data
- **Advanced Error Analytics**: Automated insights vs manual analysis
- **Proactive Error Management**: Pattern detection and remediation tracking

---

**Implementation Status**: ✅ **BUILT-IN DCF FUNCTIONALITY**  
**Complexity**: Low (Framework provides enhanced functionality)  
**Dependencies**: DCF error framework, enhanced error repository  
**Priority**: 🟡 **IMPORTANT** - Centralized error management and analysis