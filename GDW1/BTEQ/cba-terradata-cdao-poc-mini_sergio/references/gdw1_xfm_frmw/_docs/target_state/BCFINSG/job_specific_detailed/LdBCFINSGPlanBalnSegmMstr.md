# LdBCFINSGPlanBalnSegmMstr - dbt DCF Implementation

## Overview

This document details the dbt implementation of the **high-performance bulk loading** from DataStage `LdBCFINSGPlanBalnSegmMstr` job. This represents the final data loading phase with Snowflake-optimized bulk loading replacing Teradata FastLoad capabilities.

**Original DataStage Complexity**: Teradata FastLoad with parallel processing optimization  
**Target Implementation**: Snowflake incremental materialization with DCF integration

---

## **DataStage to dbt DCF Mapping**

### **Loading Architecture Transformation**

| **DataStage Component** | **dbt DCF Equivalent** | **Implementation** |
|------------------------|------------------------|-------------------|
| **Sequential File Reader** | `{{ ref('int_bcfinsg_validated') }}` | dbt model dependency |
| **Teradata FastLoad** | Snowflake incremental merge | Cloud-native bulk loading |
| **Parallel Processing** | Snowflake auto-scaling | Automatic parallelization |
| **Load Statistics** | DCF execution metrics | Built-in audit framework |
| **Error Handling** | dbt error capture | Integrated error processing |

---

## **Critical Loading Strategy Implementation**

### **1. Snowflake Bulk Loading Optimization**

**Current DataStage Logic**: Teradata FastLoad with session optimization  
**Target Implementation**: Snowflake incremental merge with value-based comparison

```sql
-- models/marts/core/fct_plan_baln_segm_mstr.sql
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['account_hash_key', 'plan_code', 'segment_code', 'effective_date'],
    database=var('target_database'),
    schema=var('target_schema'), 
    tags=['bcfinsg', 'marts', 'fact_table', 'stream_bcfinsg'],
    cluster_by=['effective_date', 'account_hash_key'],
    
    -- Snowflake optimization
    snowflake_warehouse=var('loading_warehouse', 'LOAD_WH'),
    query_tag='BCFINSG_BULK_LOAD',
    
    pre_hook=[
        "{{ validate_stream_status(var('stream_name')) }}",
        "{{ register_process_start(this.name, var('stream_name')) }}",
        "{{ pre_load_validation() }}"
    ],
    post_hook=[
        "{{ register_process_completion(this.name, var('stream_name')) }}",
        "{{ log_load_statistics(this.name) }}",
        "{{ validate_load_completion() }}"
    ]
) }}
```

### **2. High-Performance Loading Configuration**

**Current DataStage Logic**: Multiple Teradata sessions with FastLoad  
**Target Implementation**: Snowflake warehouse sizing and optimization

```sql
-- Pre-load optimization
{% if is_incremental() %}
    -- Optimize for incremental loading
    {{ config(
        snowflake_warehouse='LOAD_WH_LARGE',
        query_tag='BCFINSG_INCREMENTAL_LOAD'
    ) }}
{% else %}
    -- Optimize for full load
    {{ config(
        snowflake_warehouse='LOAD_WH_XLARGE', 
        query_tag='BCFINSG_FULL_LOAD'
    ) }}
{% endif %}
```

### **3. Value-Based Merge Strategy**

**Current DataStage Logic**: Bulk insert operations  
**Target Implementation**: Intelligent merge with value comparison

```sql
WITH source_data AS (
    SELECT 
        -- Business keys
        {{ dbt_utils.generate_surrogate_key(['account_number', 'plan_code']) }} as account_hash_key,
        account_number,
        plan_code,
        segment_code,
        effective_date,
        
        -- Business attributes  
        credit_limit,
        current_balance,
        total_balance,
        interest_rate,
        payment_status_category,
        
        -- All transformed date fields
        bcf_dt_spc_trms_ed,
        bcf_dt_int_defr,
        bcf_dt_pymt_defr,
        bcf_dt_frst_trns,
        bcf_dt_paid_off,
        bcf_dt_lst_pymt,
        bcf_dt_lst_maint,
        bcf_plan_due_dt,
        bcf_dt_end_intrst_free,
        bcf_dt_migrate,
        bcf_fl_lst_installmnt_dt,
        bcf_sched_payoff_dt,
        bcf_actual_payoff_dt,
        bcf_dt_install_term_chg,
        bcf_dt_install_paid,
        bcf_projected_payoff_dt,
        bcf_dispute_old_dt,
        
        -- Quality and control
        data_quality_score,
        ctlfw_delta_action_code,
        
        -- Audit metadata
        transformation_timestamp,
        batch_id,
        source_file_name,
        source_row_number,
        
        -- DCF audit columns
        {{ dcf_audit_columns('fct_plan_baln_segm_mstr') }}
        
    FROM {{ ref('int_bcfinsg_validated') }}
    
    {% if is_incremental() %}
        -- Incremental loading filter
        WHERE transformation_timestamp > (
            SELECT COALESCE(MAX(transformation_timestamp), '1900-01-01') 
            FROM {{ this }}
        )
        -- Also include records for specific business date if provided
        {% if var('run_stream_process_date', None) %}
            OR DATE(effective_date) = '{{ var("run_stream_process_date") }}'
        {% endif %}
    {% endif %}
),

load_optimized_data AS (
    SELECT *,
        -- Load optimization fields
        CASE 
            WHEN ctlfw_delta_action_code = 2 THEN transformation_timestamp  -- Delete: set expiry
            ELSE '9999-12-31 23:59:59'::TIMESTAMP_NTZ(6)                    -- Active: set to max
        END as record_expiry_timestamp,
        
        -- Load statistics for monitoring
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ(6) as load_timestamp,
        '{{ var("stream_name", "BCFINSG_PLAN_BALN_SEGM_LOAD") }}' as load_stream_name,
        {{ var("ods_batch_id") }} as load_batch_id,
        
        -- Performance tracking
        ROW_NUMBER() OVER (ORDER BY account_hash_key) as load_sequence_number,
        
    FROM source_data
    WHERE final_processing_status = 'READY_FOR_LOADING'
)

SELECT * FROM load_optimized_data
```

---

## **Advanced Merge Logic Implementation**

### **Value-Based Comparison (TCF Pattern)**

```sql
{% if is_incremental() %}

-- Advanced merge with value-based comparison
MERGE INTO {{ this }} as target
USING (
    SELECT * FROM load_optimized_data
) as source
ON target.account_hash_key = source.account_hash_key
   AND target.plan_code = source.plan_code  
   AND target.segment_code = source.segment_code
   AND target.effective_date = source.effective_date

-- DELETE: Remove records marked for deletion
WHEN MATCHED 
  AND source.ctlfw_delta_action_code = 2
THEN DELETE

-- UPDATE: Only when values actually changed (performance optimization)
WHEN MATCHED 
  AND source.ctlfw_delta_action_code = 1
  AND (
      NVL(target.credit_limit, -999999) != NVL(source.credit_limit, -999999)
      OR NVL(target.current_balance, -999999) != NVL(source.current_balance, -999999)
      OR NVL(target.total_balance, -999999) != NVL(source.total_balance, -999999)
      OR NVL(target.interest_rate, -999) != NVL(source.interest_rate, -999)
      OR NVL(target.payment_status_category, 'NULL') != NVL(source.payment_status_category, 'NULL')
      OR target.data_quality_score != source.data_quality_score
      -- Add all critical business fields for comparison
  )
THEN UPDATE SET
    credit_limit = source.credit_limit,
    current_balance = source.current_balance,
    total_balance = source.total_balance,
    interest_rate = source.interest_rate,
    payment_status_category = source.payment_status_category,
    data_quality_score = source.data_quality_score,
    
    -- Update all date fields
    bcf_dt_spc_trms_ed = source.bcf_dt_spc_trms_ed,
    bcf_dt_int_defr = source.bcf_dt_int_defr,
    bcf_dt_pymt_defr = source.bcf_dt_pymt_defr,
    -- ... all other date fields
    
    -- Update audit columns
    load_timestamp = source.load_timestamp,
    transformation_timestamp = source.transformation_timestamp,
    UPD_TS = CURRENT_TIMESTAMP()::TIMESTAMP_NTZ(6),
    UPD_USER_ID = '{{ env_var("DBT_USER", "dbt_service") }}',
    record_expiry_timestamp = source.record_expiry_timestamp

-- INSERT: New records
WHEN NOT MATCHED 
  AND source.ctlfw_delta_action_code = 1
THEN INSERT (
    account_hash_key, account_number, plan_code, segment_code, effective_date,
    credit_limit, current_balance, total_balance, interest_rate, payment_status_category,
    -- All date fields
    bcf_dt_spc_trms_ed, bcf_dt_int_defr, bcf_dt_pymt_defr, bcf_dt_frst_trns,
    bcf_dt_paid_off, bcf_dt_lst_pymt, bcf_dt_lst_maint, bcf_plan_due_dt,
    bcf_dt_end_intrst_free, bcf_dt_migrate, bcf_fl_lst_installmnt_dt,
    bcf_sched_payoff_dt, bcf_actual_payoff_dt, bcf_dt_install_term_chg,
    bcf_dt_install_paid, bcf_projected_payoff_dt, bcf_dispute_old_dt,
    -- Quality and control
    data_quality_score, ctlfw_delta_action_code,
    -- Audit metadata
    transformation_timestamp, batch_id, source_file_name, source_row_number,
    load_timestamp, load_stream_name, load_batch_id, load_sequence_number,
    record_expiry_timestamp,
    -- DCF audit columns
    EFFT_TS, EXPY_TS, PRCS_NAME, STRM_NAME, STRM_ID,
    INS_TS, UPD_TS, INS_USER_ID, UPD_USER_ID, RECORD_DELETED_FLAG
) VALUES (
    source.account_hash_key, source.account_number, source.plan_code, source.segment_code, source.effective_date,
    source.credit_limit, source.current_balance, source.total_balance, source.interest_rate, source.payment_status_category,
    -- All date field values
    source.bcf_dt_spc_trms_ed, source.bcf_dt_int_defr, source.bcf_dt_pymt_defr, source.bcf_dt_frst_trns,
    source.bcf_dt_paid_off, source.bcf_dt_lst_pymt, source.bcf_dt_lst_maint, source.bcf_plan_due_dt,
    source.bcf_dt_end_intrst_free, source.bcf_dt_migrate, source.bcf_fl_lst_installmnt_dt,
    source.bcf_sched_payoff_dt, source.bcf_actual_payoff_dt, source.bcf_dt_install_term_chg,
    source.bcf_dt_install_paid, source.bcf_projected_payoff_dt, source.bcf_dispute_old_dt,
    -- Quality and control values
    source.data_quality_score, source.ctlfw_delta_action_code,
    -- Audit metadata values
    source.transformation_timestamp, source.batch_id, source.source_file_name, source.source_row_number,
    source.load_timestamp, source.load_stream_name, source.load_batch_id, source.load_sequence_number,
    source.record_expiry_timestamp,
    -- DCF audit column values
    source.EFFT_TS, source.EXPY_TS, source.PRCS_NAME, source.STRM_NAME, source.STRM_ID,
    source.INS_TS, source.UPD_TS, source.INS_USER_ID, source.UPD_USER_ID, source.RECORD_DELETED_FLAG
)

{% endif %}
```

---

## **Load Performance Optimization**

### **1. Snowflake Warehouse Configuration**

```sql
-- macros/configure_load_warehouse.sql
{% macro configure_load_warehouse() %}
    {% set load_volume = var('expected_load_volume', 'MEDIUM') %}
    {% if load_volume == 'SMALL' %}
        {{ return('LOAD_WH_SMALL') }}
    {% elif load_volume == 'LARGE' %}  
        {{ return('LOAD_WH_LARGE') }}
    {% elif load_volume == 'XLARGE' %}
        {{ return('LOAD_WH_XLARGE') }}
    {% else %}
        {{ return('LOAD_WH_MEDIUM') }}
    {% endif %}
{% endmacro %}
```

### **2. Load Statistics and Monitoring**

```sql
-- macros/log_load_statistics.sql
{% macro log_load_statistics(model_name) %}
    INSERT INTO {{ var('dcf_database') }}.{{ var('dcf_schema') }}.DCF_T_EXEC_LOG (
        STRM_ID,
        STRM_NAME,  
        PRCS_NAME,
        EXEC_START_TS,
        EXEC_END_TS,
        EXEC_STATUS,
        RECORDS_PROCESSED,
        RECORDS_INSERTED,
        RECORDS_UPDATED,
        RECORDS_DELETED,
        LOAD_DURATION_SECONDS,
        WAREHOUSE_USED,
        CREDITS_CONSUMED
    )
    SELECT 
        {{ var('stream_id', 1490) }},
        '{{ var("stream_name", "BCFINSG_PLAN_BALN_SEGM_LOAD") }}',
        '{{ model_name }}',
        '{{ run_started_at }}',
        CURRENT_TIMESTAMP(),
        'SUCCESS',
        (SELECT COUNT(*) FROM {{ this }}),
        -- Calculate insert/update/delete counts
        {{ get_merge_statistics() }},
        DATEDIFF('second', '{{ run_started_at }}', CURRENT_TIMESTAMP()),
        '{{ configure_load_warehouse() }}',
        {{ get_credits_consumed() }}
{% endmacro %}
```

---

## **Load Validation and Quality Assurance**

### **Pre-Load Validation**

```sql
-- macros/pre_load_validation.sql
{% macro pre_load_validation() %}
    -- Validate source data availability
    {% set source_count %}
        SELECT COUNT(*) FROM {{ ref('int_bcfinsg_validated') }}
        WHERE final_processing_status = 'READY_FOR_LOADING'
    {% endset %}
    
    {% if execute %}
        {% set source_count_result = run_query(source_count) %}
        {% set record_count = source_count_result.columns[0].values()[0] %}
        
        {% if record_count == 0 %}
            {{ exceptions.raise_compiler_error("No records ready for loading. Check upstream processing.") }}
        {% endif %}
        
        {{ log("Pre-load validation: " ~ record_count ~ " records ready for loading", info=true) }}
    {% endif %}
{% endmacro %}
```

### **Post-Load Validation**

```sql
-- macros/validate_load_completion.sql
{% macro validate_load_completion() %}
    -- Validate load completion and data integrity
    {% set validation_sql %}
        SELECT 
            COUNT(*) as total_records,
            COUNT(CASE WHEN data_quality_score >= 85 THEN 1 END) as high_quality_records,
            AVG(data_quality_score) as avg_quality_score,
            MAX(load_timestamp) as latest_load_timestamp
        FROM {{ this }}
        WHERE DATE(load_timestamp) = CURRENT_DATE()
    {% endset %}
    
    {% if execute %}
        {% set validation_result = run_query(validation_sql) %}
        {% for row in validation_result %}
            {{ log("Load validation - Total records: " ~ row[0] ~ 
                   ", High quality: " ~ row[1] ~ 
                   ", Avg quality: " ~ row[2] ~ 
                   ", Latest load: " ~ row[3], info=true) }}
        {% endfor %}
    {% endif %}
{% endmacro %}
```

---

## **Implementation Comparison**

| **Aspect** | **DataStage Original** | **dbt DCF Implementation** |
|------------|----------------------|---------------------------|
| **Loading Method** | Teradata FastLoad | Snowflake incremental merge |
| **Parallel Processing** | Fixed node configuration | Auto-scaling warehouse |
| **Session Management** | Manual session optimization | Automatic Snowflake optimization |
| **Error Handling** | Separate error processing | Integrated dbt error capture |
| **Load Statistics** | FastLoad log parsing | DCF execution metrics |
| **Performance Monitoring** | Manual log analysis | Real-time DCF dashboard |
| **Value Comparison** | Basic insert/update | Intelligent value-based merge |
| **Scalability** | Hardware dependent | Cloud auto-scaling |

---

## **Key Benefits of dbt Implementation**

1. **🚀 Cloud Performance**: Snowflake auto-scaling vs fixed Teradata sessions
2. **🎯 Intelligent Loading**: Value-based merge prevents unnecessary updates
3. **📊 Integrated Monitoring**: DCF metrics vs separate log file parsing
4. **⚡ Auto-Optimization**: Warehouse scaling vs manual session tuning
5. **🔄 Version Control**: SQL-based configuration vs DataStage GUI
6. **🛠️ Maintenance**: dbt model updates vs DataStage job modifications
7. **💰 Cost Efficiency**: Pay-per-use vs fixed infrastructure costs

---

## **Load Performance Expectations**

- **Small Volume** (< 100K records): LOAD_WH_SMALL, ~2-5 minutes
- **Medium Volume** (100K - 1M records): LOAD_WH_MEDIUM, ~5-15 minutes  
- **Large Volume** (1M - 10M records): LOAD_WH_LARGE, ~15-45 minutes
- **Extra Large Volume** (> 10M records): LOAD_WH_XLARGE, ~45+ minutes

---

**Implementation Status**: ✅ **DESIGN COMPLETE**  
**Complexity**: High (Performance optimization required)  
**Dependencies**: Transformation model, DCF framework, Snowflake warehouses  
**Priority**: 🔴 **CRITICAL** - Required for data loading functionality