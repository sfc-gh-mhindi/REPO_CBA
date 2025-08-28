-- =====================================================================
-- DBT Model: DERV_ACCT_PATY_01_SP_GET_BTCH_KEY
-- Converted from BTEQ: DERV_ACCT_PATY_01_SP_GET_BTCH_KEY.sql
-- Category: derived_account_party
-- Original Size: 1.7KB, 63 lines
-- Complexity Score: 41
-- Generated: 2025-08-21 13:48:49
-- =====================================================================

{{ intermediate_model_config() }}

-- This model performs data modification operations
-- Execute via post-hook or separate run

{{ config(
    materialized='table',
    post_hook=[
        "

------------------------------------------------------------------------------
-- Object Name             :  DERV_ACCT_PATY_01_SP_GET_BTCH_KEY.sql
-- Object Type             :  BTEQ

--                           
-- Description             :  Call a stored procedure to obtain batch key for the extract date  
--     
--
--  Ver  Date       Modified By            Description
--  ---- ---------- ---------------------- -----------------------------------
--  1.0  24/07/2013 Helen Zak              Initial Version
------------------------------------------------------------------------------
--
-- This info is for CBM use only
-- $LastChangedBy: zakhe $
-- $LastChangedDate: 2013-07-25 12:29:28 +1000 (Thu, 25 Jul 2013) $
-- $LastChangedRevision: 12363 $
--


-- Remove the existing batch key file



-- Get extract date as set by the datawatcher and get a batch key for that date


USING 
( EXTR_D VARCHAR(10) )
CALL {{ bteq_var(\"STARMACRDB\") }}.SP_GET_BTCH_KEY(     
  '{{ bteq_var(\"SRCE_SYST_M\") }}'
  ,CAST(:EXTR_D AS DATE) (FORMAT'YYYYMMDD')(CHAR(8))              
  ,IBATCHKEY (CHAR(80))
); 

/* NOTE: BTCH_KEY is returned as a string in the format where     */
/* First 24 characters ignored then next 10 are the actual number */

		


 
"
    ]
) }}

-- Placeholder query for DBT execution
SELECT 1 as operation_complete
