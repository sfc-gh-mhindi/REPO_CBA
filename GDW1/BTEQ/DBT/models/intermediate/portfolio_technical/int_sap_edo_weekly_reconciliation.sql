/*
  GDW1 SAP EDO Weekly Reconciliation Model
  
  Migrated from: BTEQ_SAP_EDO_WKLY_LOAD.sql (partial - core reconciliation logic)
  
  Description: 
  Weekly reconciliation process for SAP EDO data
  Identifies missing accounts and reconciles differences between systems
  
  Original Logic:
  - Create multiple volatile tables for different reconciliation steps
  - Complex joins between account products, offers, and relationships
  - Handle HLS and SAP account differences
  - Identify missing records and mismatches
  
  DBT Approach:
  - Break complex logic into CTEs for readability
  - Focus on core reconciliation between MC (Master Card) and participant accounts
  - Maintain business logic while simplifying volatile table approach
  - Add proper data quality controls and audit tracking
  
  DCF Pattern: Intermediate processing table
  Stream: GDW1_PRTF_TECH_PROCESSING
*/

{{ 
  intermediate_model_config(
    process_type='sap_edo_weekly_reconciliation',
    stream_name=var('portfolio_stream')
  )
}}

WITH process_date AS (
    SELECT {{ current_date_minus(1) }} AS BUSINESS_DATE
),

-- Master Card reconciliation base data
edo_mc_reconciliation AS (
    SELECT DISTINCT 
        A.ACCT_I,
        TRIM(A.PDCT_N) AS MC_PDCT_N,
        B.OFFR_I,
        B.ACCT_OFFR_STRT_D,
        B.ACCT_OFFR_END_D,
        C.OBJC_ACCT_I,
        TRIM(D.PDCT_N) AS PARTICIPANT_PDCT_N,
        
        -- Add process tracking
        PD.BUSINESS_DATE,
        {{ generate_process_key() }} AS PROCESS_KEY
        
    FROM process_date PD
    CROSS JOIN (
        SELECT * 
        FROM {{ bteq_var('VTECH') }}.ACCT_PDCT 
        WHERE PDCT_N = '1603' 
          AND ACCT_I IN (
              SELECT ACCT_I 
              FROM {{ bteq_var('VTECH') }}.ACCT_BASE 
              WHERE ACCT_I LIKE 'SAPMC%' 
                AND COALESCE(CLSE_D, '9999-12-31'::DATE) = '9999-12-31'::DATE
          )
          AND {{ current_date_minus(1) }} BETWEEN EFFT_D AND EXPY_D
    ) A 
    
    LEFT JOIN (
        SELECT * 
        FROM {{ bteq_var('VTECH') }}.ACCT_OFFR 
        WHERE ACCT_OFFR_END_D = '9999-12-31'::DATE 
          AND {{ current_date_minus(1) }} BETWEEN EFFT_D AND EXPY_D
    ) B ON A.ACCT_I = B.ACCT_I 
    
    INNER JOIN (
        SELECT * 
        FROM {{ bteq_var('VTECH') }}.ACCT_REL 
        WHERE Rel_c LIKE 'MC%' 
          AND {{ current_date_minus(1) }} BETWEEN EFFT_D AND EXPY_D
    ) C ON A.ACCT_I = C.SUBJ_ACCT_I 
    
    LEFT JOIN (
        SELECT * 
        FROM {{ bteq_var('VTECH') }}.ACCT_PDCT 
        WHERE ACCT_I IN (
            SELECT ACCT_I 
            FROM {{ bteq_var('VTECH') }}.ACCT_BASE 
            WHERE (ACCT_I LIKE 'HLSHL%' OR ACCT_I LIKE 'SAPSP%')
              AND COALESCE(CLSE_D, '9999-12-31'::DATE) = '9999-12-31'::DATE
        )
        AND {{ current_date_minus(1) }} BETWEEN EFFT_D AND EXPY_D
    ) D ON D.ACCT_I = C.OBJC_ACCT_I
),

-- HLS differences between ODS and GDW
hls_differences AS (
    SELECT DISTINCT
        'HLS_DIFF_ODS_GDW' AS DIFF_TYPE,
        OFFSET_MC_ID,
        OFFSET_MC_NUMBER,
        PARTICIPATE_ACCOUNT_NUMBER,
        PARTICIPATE_ACCOUNT_SP,
        DATE_OF_LINKAGE,
        EXPIRE_OF_LINKAGE,
        PACKAGE_STATUS,
        
        -- Add reconciliation flags
        CASE 
            WHEN EDO.ACCT_I IS NULL THEN 'MISSING_IN_GDW'
            ELSE 'FOUND_IN_GDW'
        END AS RECONCILIATION_STATUS,
        
        {{ gdw1_business_date() }} AS PROCESS_DATE,
        {{ generate_process_key() }} AS PROCESS_KEY
        
    FROM {{ ref('int_offset_mc_report') }} OFFSET  -- Assuming this model exists
    LEFT JOIN edo_mc_reconciliation EDO 
        ON CAST(SUBSTR(EDO.ACCT_I, 10) AS NUMBER) = CAST(OFFSET.OFFSET_MC_NUMBER AS NUMBER)
        AND CAST(SUBSTR(EDO.OBJC_ACCT_I, 6) AS NUMBER) = CAST(OFFSET.PARTICIPATE_ACCOUNT_NUMBER AS NUMBER)
        AND SUBSTR(EDO.OBJC_ACCT_I, 1, 3) = 'HLS'
    
    WHERE OFFSET.PARTICIPATE_ACCOUNT_SOURCE = 'HLS' 
      AND OFFSET.DATE_OF_LINKAGE <= {{ current_date_minus(2) }}
      AND OFFSET.EXPIRE_OF_LINKAGE >= {{ current_date_minus(2) }}
      AND OFFSET.PACKAGE_STATUS <> 'CLOSED'
      AND EDO.ACCT_I IS NULL  -- Missing records only
),

-- SAP differences between ODS and GDW  
sap_differences AS (
    SELECT DISTINCT
        'SAP_DIFF_ODS_GDW' AS DIFF_TYPE,
        OFFSET_MC_ID,
        OFFSET_MC_NUMBER,
        PARTICIPATE_ACCOUNT_BSB,
        PARTICIPATE_ACCOUNT_NUMBER,
        PARTICIPATE_ACCOUNT_SP,
        DATE_OF_LINKAGE,
        EXPIRE_OF_LINKAGE,
        PACKAGE_STATUS,
        
        -- Add reconciliation flags
        CASE 
            WHEN EDO.ACCT_I IS NULL THEN 'MISSING_IN_GDW'
            ELSE 'FOUND_IN_GDW'
        END AS RECONCILIATION_STATUS,
        
        {{ gdw1_business_date() }} AS PROCESS_DATE,
        {{ generate_process_key() }} AS PROCESS_KEY
        
    FROM {{ ref('int_offset_mc_report') }} OFFSET  -- Assuming this model exists
    LEFT JOIN edo_mc_reconciliation EDO 
        ON CAST(SUBSTR(EDO.ACCT_I, 10) AS NUMBER) = CAST(OFFSET.OFFSET_MC_NUMBER AS NUMBER)
        AND CAST(SUBSTR(EDO.OBJC_ACCT_I, 8, 6) AS NUMBER) = CAST(OFFSET.PARTICIPATE_ACCOUNT_BSB AS NUMBER)
        AND CAST(SUBSTR(EDO.OBJC_ACCT_I, 14) AS NUMBER) = CAST(OFFSET.PARTICIPATE_ACCOUNT_NUMBER AS NUMBER)
        AND SUBSTR(EDO.OBJC_ACCT_I, 1, 3) = 'SAP'
    
    WHERE OFFSET.PARTICIPATE_ACCOUNT_SOURCE = 'SAP' 
      AND OFFSET.DATE_OF_LINKAGE <= {{ current_date_minus(2) }}
      AND OFFSET.EXPIRE_OF_LINKAGE >= {{ current_date_minus(2) }}
      AND OFFSET.PACKAGE_STATUS <> 'CLOSED'
      AND EDO.ACCT_I IS NULL  -- Missing records only
)

-- Final consolidated reconciliation output
SELECT 
    DIFF_TYPE,
    OFFSET_MC_ID,
    OFFSET_MC_NUMBER,
    PARTICIPATE_ACCOUNT_BSB,
    PARTICIPATE_ACCOUNT_NUMBER,
    PARTICIPATE_ACCOUNT_SP,
    DATE_OF_LINKAGE,
    EXPIRE_OF_LINKAGE,
    PACKAGE_STATUS,
    RECONCILIATION_STATUS,
    PROCESS_DATE,
    PROCESS_KEY,
    
    -- Add DCF audit columns
    {{ gdw1_audit_columns() }}

FROM (
    SELECT 
        DIFF_TYPE,
        OFFSET_MC_ID,
        OFFSET_MC_NUMBER,
        NULL AS PARTICIPATE_ACCOUNT_BSB,
        PARTICIPATE_ACCOUNT_NUMBER,
        PARTICIPATE_ACCOUNT_SP,
        DATE_OF_LINKAGE,
        EXPIRE_OF_LINKAGE,
        PACKAGE_STATUS,
        RECONCILIATION_STATUS,
        PROCESS_DATE,
        PROCESS_KEY
    FROM hls_differences
    
    UNION ALL
    
    SELECT 
        DIFF_TYPE,
        OFFSET_MC_ID,
        OFFSET_MC_NUMBER,
        PARTICIPATE_ACCOUNT_BSB,
        PARTICIPATE_ACCOUNT_NUMBER,
        PARTICIPATE_ACCOUNT_SP,
        DATE_OF_LINKAGE,
        EXPIRE_OF_LINKAGE,
        PACKAGE_STATUS,
        RECONCILIATION_STATUS,
        PROCESS_DATE,
        PROCESS_KEY
    FROM sap_differences
)

-- Order for consistent output
ORDER BY DIFF_TYPE, OFFSET_MC_ID 