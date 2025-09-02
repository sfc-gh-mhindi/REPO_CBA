-- =====================================================================================
-- PROCESS KEY ISSUE ANALYSIS & SOLUTION
-- =====================================================================================
-- PROBLEM: PROS_KEY_EFFT_I is coming as NULL or 1 instead of proper process keys
-- =====================================================================================

-- =====================================================================================
-- CURRENT PROBLEMS:
-- =====================================================================================

-- PROBLEM 1: Hardcoded 0 in tmpapptdeptds__xfmpl_appfrmext.sql (line 17)
-- CURRENT (WRONG):
/*
0 AS PROS_KEY_EFFT_I,  -- 🚨 HARDCODED!
*/

-- PROBLEM 2: Hardcoded 1 in dbt_project.yml (lines 131, 139) 
-- CURRENT (WRONG):
/*
refr_pk: 1  # 🚨 HARDCODED!
*/

-- =====================================================================================
-- PROPER SOLUTION: DYNAMIC PROCESS KEY GENERATION
-- =====================================================================================

-- SOLUTION 1: Fix the Model to Use refr_pk Variable
-- CORRECTED tmpapptdeptds__xfmpl_appfrmext.sql (line 17):
/*
{{ cvar("refr_pk") }} AS PROS_KEY_EFFT_I,  -- ✅ Use dynamic variable
*/

-- SOLUTION 2: Implement Proper Process Key Generation
-- The refr_pk should be generated like this:

-- OPTION A: Pre-hook to Generate Process Key
/*
pre_hook=[
    "
    -- Generate next process key from UTIL_PROS_ISAC
    MERGE INTO {{ cvar('intermediate_db') }}.{{ cvar('files_schema') }}.UTIL_PROS_ISAC AS target
    USING (
        SELECT 
            (SELECT COALESCE(MAX(PROS_KEY_I), 0) + 1 FROM {{ cvar('intermediate_db') }}.{{ cvar('files_schema') }}.UTIL_PROS_ISAC) AS PROS_KEY_I,
            'CSE_CPL_BUS_APP_DEPT_APPT' AS CONV_M,
            'DBT' AS CONV_TYPE_M,
            CURRENT_TIMESTAMP AS PROS_RQST_S,
            CURRENT_TIMESTAMP AS PROS_LAST_RQST_S,
            1 AS PROS_RQST_Q,
            TO_DATE('{{ cvar("etl_process_dt") }}', 'YYYYMMDD') AS BTCH_RUN_D,
            NULL AS BTCH_KEY_I,
            'SNOWFLAKE' AS SRCE_SYST_M,
            'CSE_CPL_BUS_APP' AS SRCE_M,
            'DEPT_APPT' AS TRGT_M,
            'N' AS SUCC_F,
            'N' AS COMT_F
    ) AS source
    ON FALSE  -- Always insert new record
    WHEN NOT MATCHED THEN
        INSERT (PROS_KEY_I, CONV_M, CONV_TYPE_M, PROS_RQST_S, PROS_LAST_RQST_S, PROS_RQST_Q, 
                BTCH_RUN_D, BTCH_KEY_I, SRCE_SYST_M, SRCE_M, TRGT_M, SUCC_F, COMT_F)
        VALUES (source.PROS_KEY_I, source.CONV_M, source.CONV_TYPE_M, source.PROS_RQST_S, 
                source.PROS_LAST_RQST_S, source.PROS_RQST_Q, source.BTCH_RUN_D, source.BTCH_KEY_I,
                source.SRCE_SYST_M, source.SRCE_M, source.TRGT_M, source.SUCC_F, source.COMT_F)
    "
]
*/

-- OPTION B: Use dbt-utils.generate_surrogate_key or similar macro
-- This is simpler but may not integrate with existing process framework

-- OPTION C: Set refr_pk in dbt_project.yml to query process key table
-- CORRECTED dbt_project.yml:
/*
refr_pk: >
  (SELECT COALESCE(MAX(PROS_KEY_I), 0) + 1 
   FROM {{ cvar('intermediate_db') }}.{{ cvar('files_schema') }}.UTIL_PROS_ISAC 
   WHERE CONV_M = 'CSE_CPL_BUS_APP_DEPT_APPT' 
   AND BTCH_RUN_D = TO_DATE('{{ cvar("etl_process_dt") }}', 'YYYYMMDD'))
*/

-- =====================================================================================
-- RECOMMENDED IMPLEMENTATION STEPS:
-- =====================================================================================

-- STEP 1: Create/Update Process Key Generation Model
-- Create a separate model: get_process_key__dept_appt.sql
/*
{{
  config(
    materialized='table',
    pre_hook=[
      "
      INSERT INTO {{ cvar('intermediate_db') }}.{{ cvar('files_schema') }}.UTIL_PROS_ISAC 
      (PROS_KEY_I, CONV_M, CONV_TYPE_M, PROS_RQST_S, BTCH_RUN_D, SRCE_M, TRGT_M, SUCC_F, COMT_F)
      SELECT 
        COALESCE(MAX(PROS_KEY_I), 0) + 1,
        'CSE_CPL_BUS_APP_DEPT_APPT',
        'DBT',
        CURRENT_TIMESTAMP,
        TO_DATE('{{ cvar("etl_process_dt") }}', 'YYYYMMDD'),
        'CSE_CPL_BUS_APP',
        'DEPT_APPT',
        'N',
        'N'
      FROM {{ cvar('intermediate_db') }}.{{ cvar('files_schema') }}.UTIL_PROS_ISAC
      WHERE NOT EXISTS (
        SELECT 1 FROM {{ cvar('intermediate_db') }}.{{ cvar('files_schema') }}.UTIL_PROS_ISAC 
        WHERE CONV_M = 'CSE_CPL_BUS_APP_DEPT_APPT' 
        AND BTCH_RUN_D = TO_DATE('{{ cvar("etl_process_dt") }}', 'YYYYMMDD')
      )
      "
    ]
  )
}}

SELECT 
  PROS_KEY_I as refr_pk,
  CONV_M,
  BTCH_RUN_D
FROM {{ cvar('intermediate_db') }}.{{ cvar('files_schema') }}.UTIL_PROS_ISAC 
WHERE CONV_M = 'CSE_CPL_BUS_APP_DEPT_APPT' 
AND BTCH_RUN_D = TO_DATE('{{ cvar("etl_process_dt") }}', 'YYYYMMDD')
ORDER BY PROS_KEY_I DESC
LIMIT 1
*/

-- STEP 2: Update tmpapptdeptds__xfmpl_appfrmext.sql
-- Change line 17 from:
--   0 AS PROS_KEY_EFFT_I,
-- To:
--   (SELECT refr_pk FROM {{ ref('get_process_key__dept_appt') }}) AS PROS_KEY_EFFT_I,

-- STEP 3: Add dependency in model
-- Add ref() to the process key model to ensure proper execution order

-- STEP 4: Update Success Flag
-- Add post_hook to mark process as successful:
/*
post_hook=[
  "
  UPDATE {{ cvar('intermediate_db') }}.{{ cvar('files_schema') }}.UTIL_PROS_ISAC 
  SET SUCC_F = 'Y', 
      COMT_F = 'Y',
      COMT_S = CURRENT_TIMESTAMP,
      SYST_INS_Q = (SELECT COUNT(*) FROM {{ this }})
  WHERE CONV_M = 'CSE_CPL_BUS_APP_DEPT_APPT' 
  AND BTCH_RUN_D = TO_DATE('{{ cvar("etl_process_dt") }}', 'YYYYMMDD')
  AND PROS_KEY_I = (SELECT refr_pk FROM {{ ref('get_process_key__dept_appt') }})
  "
]
*/

-- =====================================================================================
-- EXPECTED RESULTS AFTER FIX:
-- =====================================================================================
-- ✅ PROS_KEY_EFFT_I will be unique integer (e.g., 10371358, 10371359, etc.)
-- ✅ Each batch run gets a unique process key for full audit trail
-- ✅ Process keys will match between Snowflake and Teradata patterns
-- ✅ Proper integration with existing process framework
-- ✅ Full traceability and monitoring capabilities

-- =====================================================================================
-- IMMEDIATE QUICK FIX (if full solution not possible immediately):
-- =====================================================================================
-- Update refr_pk in dbt_project.yml to a reasonable value:
-- Instead of: refr_pk: 1
-- Use: refr_pk: 10371357  # or next available process key from UTIL_PROS_ISAC

-- This will at least give you a proper process key while implementing the full solution
-- ===================================================================================== 