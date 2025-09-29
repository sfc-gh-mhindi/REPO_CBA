-- =====================================================================
-- DBT Model: ACCT_BALN_BKDT_AVG_CALL_PROC
-- Converted from BTEQ: ACCT_BALN_BKDT_AVG_CALL_PROC.sql
-- Category: account_balance
-- Original Size: 1.1KB, 35 lines
-- Complexity Score: 20
-- Generated: 2025-08-21 10:48:29
-- =====================================================================

{{ intermediate_model_config() }}

-- This model performs data modification operations
-- Execute via post-hook or separate run

{{ config(
    materialized='table',
    post_hook=[
        "

----------------------------------------------------------------------
-- $LastChangedBy: 
-- $LastChangedDate: 2012-02-28 09:08:46 +1100 (Tue, 28 Feb 2012) $
-- $LastChangedRevision: 9219 $
----------------------------------------------------------------------
------------------------------------------------------------------------------
--
--  Description :Process to calculate the Monthly Average balance.  
--This is sourcing from ACCT BALN BKDT.
--
--   Ver  Date       Modified By            Description
--  ---- ---------- ---------------------- -----------------------------------
--  1.0  2011-10-05 Suresh Vajapeyajula               Initial Version
------------------------------------------------------------------------------

CALL {{ bteq_var(\"CAD_PROD_MACRO\") }}.SP_CALC_AVRG_DAY_BALN_BKDT (
CAST(DATEADD(MONTH, -1, CURRENT_DATE) AS DATE FORMAT 'YYYYMMDD'));
 

.QUIT 0
.LOGOFF

.LABEL EXITERR
.QUIT ERRORCODE
.LOGOFF
.EXIT"
    ]
) }}

-- Placeholder query for DBT execution
SELECT 1 as operation_complete
