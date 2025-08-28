{%- set process_name = 'CSEL4_APPT_PDCT_TRANSFORM' -%}
{%- set stream_name = 'CSEL4_CPL_BUS_APP' -%}

{{
  config(
    materialized='view',
    alias='tmp_appt_pdct',
    tags=['stream_csel4', 'process_csel4_appt_pdct_transform', 'intermediate_layer', 'data_transform', 'appt_pdct'],

    post_hook=[
        "{{ log_dcf_exec_msg('INTERMEDIATE_TRANSFORM', '" ~ stream_name ~ "', 20, 'Finished creation of intermediate table tmp_appt_pdct for APPT_PDCT target') }}"
    ]
  )
}}

/*
  This model prepares data for the APPT_PDCT target table, mirroring the exact 
  column structure from ps_cld_rw.STARCADPRODDATA.APPT_PDCT.
  
  Equivalent to DataStage: XfmPL_APPFrmExt → OutTmpApptPdctDS
*/

WITH process_instance AS (
    SELECT 
        {{ get_current_process_instance_id('CSEL4_APPT_PDCT_TRANSFORM', stream_name) }} AS current_process_inst_id,
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

product_lookup AS (
    SELECT 
        s.*,
        p.pdct_n
    FROM staged_data s
    LEFT JOIN {{ ref('ref_map_cse_pack_pdct_pl') }} p
        ON s.pl_package_cat_id = p.pl_pack_cat_id
        AND p.is_current = TRUE
    WHERE s.pl_package_cat_id IS NOT NULL
),

transformed AS (
    SELECT
        -- Target table columns in exact order (mirroring ps_cld_rw.STARCADPRODDATA.APPT_PDCT)
        -- 1. APPT_PDCT_I - Application Product Identifier (populated)
        'CSE' || 'PP' || s.pl_app_id                      AS appt_pdct_i,
        
        -- 2. APPT_QLFY_C - Application Qualifier Code (populated)
        'PP'                                              AS appt_qlfy_c,
        
        -- 3. ACQR_TYPE_C - Acquisition Type Code (NULL)
        NULL::VARCHAR                                     AS acqr_type_c,
        
        -- 4. ACQR_ADHC_X - Acquisition Ad Hoc Comment (NULL)
        NULL::VARCHAR                                     AS acqr_adhc_x,
        
        -- 5. ACQR_SRCE_C - Acquisition Source Code (NULL)
        NULL::VARCHAR                                     AS acqr_srce_c,
        
        -- 6. PDCT_N - Product Number (populated)
        CAST(s.pdct_n AS NUMBER(38,0))                    AS pdct_n,
        
        -- 7. APPT_I - Application Identifier (populated)
        'CSE' || ':' || 'PL' || ':' || s.pl_app_id        AS appt_i,
        
        -- 8. SRCE_SYST_C - Source System Code (populated)
        'CSE'                                             AS srce_syst_c,
        
        -- 9. SRCE_SYST_APPT_PDCT_I - Source System Application Product Identifier (populated)
        s.pl_app_id                                       AS srce_syst_appt_pdct_i,
        
        -- 10. LOAN_FNDD_METH_C - Loan Funding Method Code (NULL)
        NULL::VARCHAR                                     AS loan_fndd_meth_c,
        
        -- 11. NEW_ACCT_F - New Account Flag (populated)
        'N'                                               AS new_acct_f,
        
        -- 12. BROK_PATY_I - Broker Party Identifier (NULL)
        NULL::VARCHAR                                     AS brok_paty_i,
        
        -- 13. COPY_FROM_OTHR_APPT_F - Copy From Other Application Flag (populated)
        'N'                                               AS copy_from_othr_appt_f,
        
        -- 14. EFFT_D - Effective Date (populated)
        p.current_business_date AS efft_d,
        
        -- 15. EXPY_D - Expiry Date (populated)
        CAST('9999-12-31' AS DATE)                        AS expy_d,
        
        -- 16. PROS_KEY_EFFT_I - Process Key Effective (populated)
        p.current_process_inst_id::NUMBER(10,0)           AS pros_key_efft_i,
        
        -- 17. PROS_KEY_EXPY_I - Process Key Expiry (same as effective)
        p.current_process_inst_id::NUMBER(10,0)           AS pros_key_expy_i,
        
        -- 18. EROR_SEQN_I - Error Sequence Identifier (NULL)
        NULL::NUMBER(10,0)                                AS eror_seqn_i,
        
        -- 19. JOB_COMM_CATG_C - Job Commission Category Code (NULL)
        NULL::VARCHAR                                     AS job_comm_catg_c,
        
        -- 20. DEBT_ABN_X - (NULL)
        NULL::VARCHAR                                     AS debt_abn_x,
        
        -- 21. DEBT_BUSN_M - (NULL)
        NULL::VARCHAR                                     AS debt_busn_m,
        
        -- 22. SMPL_APPT_F - (NULL)
        NULL::VARCHAR                                     AS smpl_appt_f,
        
        -- 23. APPT_PDCT_CATG_C - (NULL)
        NULL::VARCHAR                                     AS appt_pdct_catg_c,
        
        -- 24. APPT_PDCT_DURT_C - (NULL)
        NULL::VARCHAR                                     AS appt_pdct_durt_c,
        
        -- 25. ASES_D - (NULL)
        NULL::DATE                                        AS ases_d
    FROM product_lookup s
    CROSS JOIN process_instance p
),

appt_pdct_staging AS (
    SELECT
        -- Target table columns only (exact match to ps_cld_rw.STARCADPRODDATA.APPT_PDCT)
        appt_pdct_i,              -- 1. Application Product Identifier
        appt_qlfy_c,              -- 2. Application Qualifier Code
        acqr_type_c,              -- 3. Acquisition Type Code
        acqr_adhc_x,              -- 4. Acquisition Ad Hoc Comment
        acqr_srce_c,              -- 5. Acquisition Source Code
        pdct_n,                   -- 6. Product Number
        appt_i,                   -- 7. Application Identifier
        srce_syst_c,              -- 8. Source System Code
        srce_syst_appt_pdct_i,    -- 9. Source System Application Product Identifier
        loan_fndd_meth_c,         -- 10. Loan Funding Method Code
        new_acct_f,               -- 11. New Account Flag
        brok_paty_i,              -- 12. Broker Party Identifier
        copy_from_othr_appt_f,    -- 13. Copy From Other Application Flag
        efft_d,                   -- 14. Effective Date
        expy_d,                   -- 15. Expiry Date
        pros_key_efft_i,          -- 16. Process Key Effective
        pros_key_expy_i,          -- 17. Process Key Expiry
        eror_seqn_i,              -- 18. Error Sequence Identifier
        job_comm_catg_c,          -- 19. Job Commission Category Code
        debt_abn_x,               -- 20. DEBT_ABN_X
        debt_busn_m,              -- 21. DEBT_BUSN_M
        smpl_appt_f,              -- 22. SMPL_APPT_F
        appt_pdct_catg_c,         -- 23. APPT_PDCT_CATG_C
        appt_pdct_durt_c,         -- 24. APPT_PDCT_DURT_C
        ases_d                    -- 25. ASES_D
    FROM transformed
)

SELECT * FROM appt_pdct_staging
