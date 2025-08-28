{%- set process_name = 'CSEL4_DEPT_APPT_TRANSFORM' -%}
{%- set stream_name = 'CSEL4_CPL_BUS_APP' -%}

{{
  config(
    materialized='view',
    alias='tmp_dept_appt',
    tags=['stream_csel4', 'process_csel4_dept_appt_transform', 'intermediate_layer', 'data_transform', 'dept_appt'],

    post_hook=[
        "{{ log_dcf_exec_msg('INTERMEDIATE_TRANSFORM', '" ~ stream_name ~ "', 20, 'Finished creation of intermediate table tmp_dept_appt for DEPT_APPT target') }}"
    ]
  )
}}

/*
  This model prepares data for the DEPT_APPT target table, mirroring the exact 
  column structure from ps_cld_rw.STARCADPRODDATA.DEPT_APPT.
  
  Equivalent to DataStage: XfmPL_APPFrmExt → OutTmpDeptApptDS
*/

WITH process_instance AS (
    SELECT 
        {{ get_current_process_instance_id('CSEL4_DEPT_APPT_TRANSFORM', stream_name) }} AS current_process_inst_id,
        bd.BUS_DT as current_business_date
    FROM {{ dcf_database_ref() }}.DCF_T_STRM_BUS_DT bd
    WHERE bd.STRM_NAME = '{{ stream_name }}' 
      AND bd.PROCESSING_FLAG = 1
    LIMIT 1
),

-- Dependency: Ensure error checking completed successfully before proceeding
error_check_dependency AS (
    SELECT COUNT(*) as error_count
    FROM {{ ref('int_validate_cse_cpl_bus_app') }}
),

staged_data AS (
    SELECT *
    FROM {{ var('staging_database') }}.{{ var('staging_schema') }}.cse_cpl_bus_app
),

transformed AS (
    SELECT
        -- Target table columns in exact order (mirroring ps_cld_rw.STARCADPRODDATA.DEPT_APPT)
        -- 1. DEPT_I - Department Identifier (populated)
        s.nominated_branch_id                             AS dept_i,
        
        -- 2. APPT_I - Application Identifier (populated)
        'CSE' || ':' || 'PL' || ':' || s.pl_app_id        AS appt_i,
        
        -- 3. DEPT_ROLE_C - Department Role Code (populated)
        'NOMN'                                            AS dept_role_c,
        
        -- 4. EFFT_D - Effective Date (populated)
        p.current_business_date AS efft_d,
        
        -- 5. EXPY_D - Expiry Date (populated)
        CAST('9999-12-31' AS DATE)                        AS expy_d,
        
        -- 6. PROS_KEY_EFFT_I - Process Key Effective (populated)
        p.current_process_inst_id::NUMBER(10,0)           AS pros_key_efft_i,
        
        -- 7. PROS_KEY_EXPY_I - Process Key Expiry (same as effective)
        p.current_process_inst_id::NUMBER(10,0)           AS pros_key_expy_i,
        
        -- 8. EROR_SEQN_I - Error Sequence Identifier (NULL)
        NULL::NUMBER(10,0)                                AS eror_seqn_i,
        
        -- 9. BRCH_N - Branch Number (NOT populated by DataStage)
        NULL::NUMBER(38,0)                                AS brch_n,
        
        -- 10. SRCE_SYST_C - Source System Code (NOT populated by DataStage)
        NULL::VARCHAR                                     AS srce_syst_c,
        
        -- 11. CHNG_REAS_C - Change Reason Code (NOT populated by DataStage)
        NULL::VARCHAR                                     AS chng_reas_c
    FROM staged_data s
    CROSS JOIN process_instance p
),

dept_appt_staging AS (
    SELECT
        -- Target table columns only (exact match to ps_cld_rw.STARCADPRODDATA.DEPT_APPT)
        dept_i,                   -- 1. Department Identifier
        appt_i,                   -- 2. Application Identifier
        dept_role_c,              -- 3. Department Role Code
        efft_d,                   -- 4. Effective Date
        expy_d,                   -- 5. Expiry Date
        pros_key_efft_i,          -- 6. Process Key Effective
        pros_key_expy_i,          -- 7. Process Key Expiry
        eror_seqn_i,              -- 8. Error Sequence Identifier
        brch_n,                   -- 9. Branch Number
        srce_syst_c,              -- 10. Source System Code
        chng_reas_c               -- 11. Change Reason Code
    FROM transformed
)

SELECT * FROM dept_appt_staging
