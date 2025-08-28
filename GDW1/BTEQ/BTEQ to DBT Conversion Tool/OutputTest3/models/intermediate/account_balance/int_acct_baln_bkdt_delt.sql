-- =====================================================================
-- DBT Model: ACCT_BALN_BKDT_DELT
-- Converted from BTEQ: ACCT_BALN_BKDT_DELT.sql
-- Category: account_balance
-- Original Size: 1.8KB, 50 lines
-- Complexity Score: 23
-- Generated: 2025-08-21 10:51:05
-- =====================================================================

{{ intermediate_model_config() }}

-- This model performs data modification operations
-- Execute via post-hook or separate run

{{ config(
    materialized='table',
    post_hook=[
        "
----------------------------------------------------------------------
-- $LastChangedBy: vajapes $
-- $LastChangedDate: 2012-02-28 09:08:57 +1100 (Tue, 28 Feb 2012) $
-- $LastChangedRevision: 9220 $
----------------------------------------------------------------------
------------------------------------------------------------------------------
--
--  Description :Delete Accts from ACCT BALN so that the modified data can be inserted in next step
--
--   Ver  Date       Modified By            Description
--  ---- ---------- ---------------------- -----------------------------------
--  1.0  2011-10-05 Suresh Vajapeyajula               Initial Version
------------------------------------------------------------------------------


DELETE BAL
/* Deleting the records from the ACCT_BALN_BKDT table. These records are modified 
as a result of applying adjustment and so will be resinserted from STG2 at next step*/
FROM
 {{ bteq_var(\"CAD_PROD_DATA\") }}.ACCT_BALN_BKDT BAL,
 {{ bteq_var(\"DDSTG\") }}.ACCT_BALN_BKDT_STG1 STG1
 WHERE 
STG1.ACCT_I = BAL.ACCT_I    
AND STG1.BALN_TYPE_C = BAL.BALN_TYPE_C                    
AND STG1.CALC_FUNC_C = BAL.CALC_FUNC_C                   
AND STG1.TIME_PERD_C = BAL.TIME_PERD_C                   
AND STG1.BKDT_EFFT_D = BAL.BKDT_EFFT_D                        
AND STG1.BKDT_EXPY_D = BAL.BKDT_EXPY_D                        
AND STG1.BALN_A = BAL.BALN_A                        
AND STG1.CALC_F = BAL.CALC_F                        
AND COALESCE(STG1.PROS_KEY_EFFT_I,0) = COALESCE(BAL.PROS_KEY_EFFT_I,0)
AND COALESCE(STG1.PROS_KEY_EXPY_I,0) = COALESCE(BAL.PROS_KEY_EXPY_I,0);


.QUIT 0
.LOGOFF
.EXIT

.LABEL EXITERR
.QUIT 1
.LOGOFF
.EXIT
"
    ]
) }}

-- Placeholder query for DBT execution
SELECT 1 as operation_complete
