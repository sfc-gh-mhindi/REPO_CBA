# XfmPlanBalnSegmMstrFromBCFINSG - dbt DCF Implementation

## Overview

This document details the dbt implementation of the **core data transformation logic** from DataStage `XfmPlanBalnSegmMstrFromBCFINSG` job. This represents the heart of the BCFINSG ETL pipeline, converting EBCDIC mainframe data into analytics-ready format using dbt and the DCF framework.

**Original DataStage Complexity**: 25,383 lines of code with 18+ output links and complex date transformations  
**Target Implementation**: dbt model with DCF integration and cloud-native optimization

---

## **DataStage to dbt DCF Mapping**

### **Core Transformation Architecture**

| **DataStage Component** | **dbt DCF Equivalent** | **Implementation** |
|------------------------|------------------------|-------------------|
| **EBCDIC File Reader** | Pre-loaded source tables | Cloud ingestion handles EBCDIC conversion |
| **Complex Transformer (18+ links)** | `int_bcfinsg_validated.sql` | SQL transformations with business logic |
| **16+ Date Transformations** | Date transformation CTEs | SQL date functions and business rules |
| **Error Segregation** | Data quality validation | Error classification and quality scoring |
| **Funnel Consolidation** | Single model output | dbt model consolidates all transformations |

---

## **Critical Business Logic Implementation**

### **1. Date Field Transformations (16+ Fields)**

**Current DataStage Logic**: Complex date conversions with business rules  
**Target Implementation**: SQL-based date transformations with error handling

```sql
-- models/intermediate/int_bcfinsg_date_transformations.sql
WITH date_transformations AS (
    SELECT *,
        -- BCF_DT_SPC_TRMS_ED (Special Terms End Date)
        CASE 
            WHEN TRIM(bcf_dt_spc_trms_ed_raw) IN ('0', '00000000', '') THEN NULL
            WHEN LENGTH(TRIM(bcf_dt_spc_trms_ed_raw)) = 8 
                AND REGEXP_LIKE(bcf_dt_spc_trms_ed_raw, '^[0-9]{8}$')
            THEN TRY_TO_DATE(bcf_dt_spc_trms_ed_raw, 'YYYYMMDD')
            ELSE NULL
        END as bcf_dt_spc_trms_ed,
        
        -- BCF_DT_INT_DEFR (Interest Deferred Date)
        CASE 
            WHEN TRIM(bcf_dt_int_defr_raw) IN ('0', '00000000', '') THEN NULL
            WHEN LENGTH(TRIM(bcf_dt_int_defr_raw)) = 8 
                AND REGEXP_LIKE(bcf_dt_int_defr_raw, '^[0-9]{8}$')
            THEN TRY_TO_DATE(bcf_dt_int_defr_raw, 'YYYYMMDD')
            ELSE NULL
        END as bcf_dt_int_defr,
        
        -- BCF_DT_PYMT_DEFR (Payment Deferred Date)
        CASE 
            WHEN TRIM(bcf_dt_pymt_defr_raw) IN ('0', '00000000', '') THEN NULL
            WHEN LENGTH(TRIM(bcf_dt_pymt_defr_raw)) = 8 
                AND REGEXP_LIKE(bcf_dt_pymt_defr_raw, '^[0-9]{8}$')
            THEN TRY_TO_DATE(bcf_dt_pymt_defr_raw, 'YYYYMMDD')
            ELSE NULL
        END as bcf_dt_pymt_defr,
        
        -- BCF_DT_FRST_TRNS (First Transaction Date)
        CASE 
            WHEN TRIM(bcf_dt_frst_trns_raw) IN ('0', '00000000', '') THEN NULL
            WHEN LENGTH(TRIM(bcf_dt_frst_trns_raw)) = 8 
                AND REGEXP_LIKE(bcf_dt_frst_trns_raw, '^[0-9]{8}$')
            THEN TRY_TO_DATE(bcf_dt_frst_trns_raw, 'YYYYMMDD')
            ELSE NULL
        END as bcf_dt_frst_trns,
        
        -- Continue for all 16+ date fields...
        -- BCF_DT_PAID_OFF, BCF_DT_LST_PYMT, BCF_DT_LST_MAINT, etc.
        
    FROM {{ ref('stg_bcfinsg_plan_baln_segm') }}
)
```

### **2. Complex Business Rule Implementation**

**Current DataStage Logic**: Credit card business rules and calculations  
**Target Implementation**: SQL business logic with comprehensive validation

```sql
-- Business rule transformations
WITH business_rules AS (
    SELECT *,
        -- Credit limit validation and adjustment
        CASE 
            WHEN credit_limit_raw < 0 THEN 0
            WHEN credit_limit_raw > 999999999 THEN 999999999
            ELSE credit_limit_raw
        END as credit_limit_adjusted,
        
        -- Interest rate calculations
        CASE 
            WHEN interest_rate_raw BETWEEN 0 AND 100 THEN interest_rate_raw / 100
            ELSE NULL
        END as interest_rate_decimal,
        
        -- Payment status derivation
        CASE 
            WHEN days_past_due = 0 THEN 'CURRENT'
            WHEN days_past_due BETWEEN 1 AND 30 THEN 'LATE_1_30'
            WHEN days_past_due BETWEEN 31 AND 60 THEN 'LATE_31_60'
            WHEN days_past_due BETWEEN 61 AND 90 THEN 'LATE_61_90'
            WHEN days_past_due > 90 THEN 'LATE_90_PLUS'
            ELSE 'UNKNOWN'
        END as payment_status_category,
        
        -- Balance calculations
        current_balance + pending_transactions as total_balance,
        
    FROM date_transformations
)
```

### **3. Error Classification and Quality Scoring**

**Current DataStage Logic**: Error segregation to UTIL_TRSF_EROR_RQM3  
**Target Implementation**: DCF error classification with quality scoring

```sql
-- Error classification and data quality
WITH quality_assessment AS (
    SELECT *,
        -- Comprehensive error classification
        CASE 
            WHEN account_number IS NULL OR LENGTH(TRIM(account_number)) = 0 
                THEN 'MISSING_ACCOUNT_NUMBER'
            WHEN NOT REGEXP_LIKE(account_number, '^[0-9]{10,20}$') 
                THEN 'INVALID_ACCOUNT_FORMAT'
            WHEN plan_code IS NULL OR LENGTH(TRIM(plan_code)) = 0 
                THEN 'MISSING_PLAN_CODE'
            WHEN credit_limit_raw IS NULL 
                THEN 'MISSING_CREDIT_LIMIT'
            WHEN current_balance IS NULL 
                THEN 'MISSING_CURRENT_BALANCE'
            ELSE 'VALID'
        END as validation_result,
        
        -- Data quality score (0-100)
        CASE 
            WHEN validation_result = 'VALID' 
             AND all_date_fields_valid = true
             AND credit_limit_adjusted IS NOT NULL
             AND interest_rate_decimal IS NOT NULL
            THEN 100
            
            WHEN validation_result = 'VALID' 
             AND (credit_limit_adjusted IS NOT NULL OR current_balance IS NOT NULL)
            THEN 85
            
            WHEN validation_result = 'VALID'
            THEN 70
            
            ELSE 0
        END as data_quality_score,
        
    FROM business_rules
)
```

---

## **Complete dbt Implementation**

### **Target Model: `int_bcfinsg_validated.sql`**

```sql
{{ config(
    materialized='table',
    tags=['bcfinsg', 'intermediate', 'transformation', 'stream_bcfinsg'],
    pre_hook=[
        "{{ validate_stream_status(var('stream_name', 'BCFINSG_PLAN_BALN_SEGM_LOAD')) }}",
        "{{ register_process_start(this.name, var('stream_name', 'BCFINSG_PLAN_BALN_SEGM_LOAD')) }}"
    ],
    post_hook=[
        "{{ register_process_completion(this.name, var('stream_name', 'BCFINSG_PLAN_BALN_SEGM_LOAD')) }}",
        "{{ log_transformation_stats(this.name) }}"
    ]
) }}

/*
    Core BCFINSG Transformation Model
    Equivalent to DataStage XfmPlanBalnSegmMstrFromBCFINSG job
    
    This model implements:
    - 16+ date field transformations with business logic
    - Complex credit card business rules and calculations  
    - Comprehensive data quality validation and error handling
    - DCF audit trail and process tracking
    - Cloud-optimized SQL transformations
*/

WITH source_data AS (
    SELECT * FROM {{ ref('stg_bcfinsg_plan_baln_segm') }}
    WHERE validation_status = 'VALID'
),

-- Date transformations (all 16+ date fields)
date_transformations AS (
    SELECT *,
        -- Transform all date fields with consistent logic
        {{ transform_bcfinsg_date('bcf_dt_spc_trms_ed_raw') }} as bcf_dt_spc_trms_ed,
        {{ transform_bcfinsg_date('bcf_dt_int_defr_raw') }} as bcf_dt_int_defr,
        {{ transform_bcfinsg_date('bcf_dt_pymt_defr_raw') }} as bcf_dt_pymt_defr,
        {{ transform_bcfinsg_date('bcf_dt_frst_trns_raw') }} as bcf_dt_frst_trns,
        {{ transform_bcfinsg_date('bcf_dt_paid_off_raw') }} as bcf_dt_paid_off,
        {{ transform_bcfinsg_date('bcf_dt_lst_pymt_raw') }} as bcf_dt_lst_pymt,
        {{ transform_bcfinsg_date('bcf_dt_lst_maint_raw') }} as bcf_dt_lst_maint,
        {{ transform_bcfinsg_date('bcf_plan_due_dt_raw') }} as bcf_plan_due_dt,
        {{ transform_bcfinsg_date('bcf_dt_end_intrst_free_raw') }} as bcf_dt_end_intrst_free,
        {{ transform_bcfinsg_date('bcf_dt_migrate_raw') }} as bcf_dt_migrate,
        {{ transform_bcfinsg_date('bcf_fl_lst_installmnt_dt_raw') }} as bcf_fl_lst_installmnt_dt,
        {{ transform_bcfinsg_date('bcf_sched_payoff_dt_raw') }} as bcf_sched_payoff_dt,
        {{ transform_bcfinsg_date('bcf_actual_payoff_dt_raw') }} as bcf_actual_payoff_dt,
        {{ transform_bcfinsg_date('bcf_dt_install_term_chg_raw') }} as bcf_dt_install_term_chg,
        {{ transform_bcfinsg_date('bcf_dt_install_paid_raw') }} as bcf_dt_install_paid,
        {{ transform_bcfinsg_date('bcf_projected_payoff_dt_raw') }} as bcf_projected_payoff_dt,
        {{ transform_bcfinsg_date('bcf_dispute_old_dt_raw') }} as bcf_dispute_old_dt,
        
    FROM source_data
),

-- Business rule transformations
business_transformations AS (
    SELECT *,
        -- Credit card business logic implementation
        {{ apply_credit_limit_rules('credit_limit_raw') }} as credit_limit_adjusted,
        {{ calculate_interest_rate('interest_rate_raw') }} as interest_rate_decimal,
        {{ derive_payment_status('days_past_due', 'payment_status_code') }} as payment_status_category,
        {{ calculate_total_balance('current_balance', 'pending_transactions') }} as total_balance,
        
        -- Business key generation
        {{ dbt_utils.generate_surrogate_key([
            'account_number', 
            'plan_code', 
            'segment_code',
            'effective_date'
        ]) }} as business_key,
        
    FROM date_transformations
),

-- Comprehensive validation and quality scoring
final_validation AS (
    SELECT *,
        -- Error classification (equivalent to DataStage error links)
        {{ classify_transformation_errors() }} as transformation_error_type,
        
        -- Data quality scoring (0-100)
        {{ calculate_data_quality_score() }} as data_quality_score,
        
        -- Record status for incremental processing
        CASE 
            WHEN transformation_error_type = 'VALID' AND data_quality_score >= 85 
            THEN 'READY_FOR_LOADING'
            WHEN transformation_error_type = 'VALID' AND data_quality_score >= 70 
            THEN 'CONDITIONAL_LOADING'
            ELSE 'REJECT_RECORD'
        END as final_processing_status,
        
        -- Delta processing support
        {{ determine_delta_action('record_status_code') }} as ctlfw_delta_action_code,
        
        -- DCF audit columns
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ(6) as transformation_timestamp,
        '{{ var("stream_name", "BCFINSG_PLAN_BALN_SEGM_LOAD") }}' as transformation_stream_name,
        {{ var("stream_id", 1490) }} as transformation_stream_id,
        
    FROM business_transformations
)

SELECT 
    -- Business keys
    account_number,
    plan_code, 
    segment_code,
    business_key,
    
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
    
    -- Business attributes
    credit_limit_adjusted as credit_limit,
    interest_rate_decimal as interest_rate,
    payment_status_category,
    total_balance,
    current_balance,
    
    -- Quality and processing metadata
    transformation_error_type,
    data_quality_score,
    final_processing_status,
    ctlfw_delta_action_code,
    
    -- Audit and lineage
    transformation_timestamp,
    transformation_stream_name,
    transformation_stream_id,
    source_file_name,
    source_row_number,
    batch_id,
    
    -- DCF audit columns
    {{ dcf_audit_columns('int_bcfinsg_validated') }}

FROM final_validation
WHERE final_processing_status IN ('READY_FOR_LOADING', 'CONDITIONAL_LOADING')

-- Log transformation completion
{{ log_transformation_summary() }}
```

---

## **Supporting Macros Required**

### **Date Transformation Macro**

```sql
-- macros/transform_bcfinsg_date.sql
{% macro transform_bcfinsg_date(date_field) %}
    CASE 
        WHEN TRIM({{ date_field }}) IN ('0', '00000000', '') THEN NULL
        WHEN LENGTH(TRIM({{ date_field }})) = 8 
            AND REGEXP_LIKE({{ date_field }}, '^[0-9]{8}$')
        THEN TRY_TO_DATE({{ date_field }}, 'YYYYMMDD')
        ELSE NULL
    END
{% endmacro %}
```

### **Business Rules Macros**

```sql
-- macros/apply_credit_limit_rules.sql
{% macro apply_credit_limit_rules(credit_field) %}
    CASE 
        WHEN {{ credit_field }} < 0 THEN 0
        WHEN {{ credit_field }} > 999999999 THEN 999999999
        ELSE {{ credit_field }}
    END
{% endmacro %}
```

---

## **Implementation Comparison**

| **Aspect** | **DataStage Original** | **dbt DCF Implementation** |
|------------|----------------------|---------------------------|
| **Lines of Code** | 25,383 lines | ~400 lines (SQL + macros) |
| **Processing Model** | EBCDIC file → 18 output links → funnel | Single SQL model with CTEs |
| **Date Transformations** | 16+ separate transformer links | Macro-based transformations |
| **Error Handling** | Separate error output links | Integrated validation logic |
| **Performance** | Parallel DataStage engine | Snowflake SQL optimization |
| **Maintenance** | Complex GUI-based job | Version-controlled SQL |
| **Audit Trail** | Oracle control tables | DCF framework integration |
| **Scalability** | Node-based scaling | Cloud auto-scaling |

---

## **Key Benefits of dbt Implementation**

1. **🎯 Simplified Architecture**: Single SQL model replaces 25,383 lines of DataStage code
2. **🔄 Version Control**: Full SQL code in Git vs proprietary DataStage format
3. **⚡ Cloud Performance**: Snowflake optimization vs legacy parallel processing
4. **🛠️ Maintainability**: SQL transformations vs complex GUI configurations
5. **📊 Integrated Audit**: DCF framework vs separate Oracle control tables
6. **🔍 Data Quality**: Built-in quality scoring vs separate error processing
7. **🚀 Scalability**: Cloud auto-scaling vs fixed infrastructure

---

**Implementation Status**: ✅ **DESIGN COMPLETE**  
**Complexity**: High (Core transformation logic)  
**Dependencies**: Staging model, DCF macros, supporting macros  
**Priority**: 🔴 **CRITICAL** - Required for pipeline functionality