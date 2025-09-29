-- =====================================================================
-- DBT Model: DERV_ACCT_PATY_01_SP_GET_PROS_KEY
-- Converted from BTEQ: DERV_ACCT_PATY_01_SP_GET_PROS_KEY.sql
-- Category: derived_account_party
-- Original Size: 2.4KB, 88 lines
-- Complexity Score: 59
-- Generated: 2025-08-21 15:50:26
-- =====================================================================

{{ intermediate_model_config() }}



------------------------------------------------------------------------------
-- Object Name             :  DERV_ACCT_PATY_01_SP_GET_PROS_KEY.sql
-- Object Type             :  BTEQ

--                           
-- Description             :  Get process key for the batch key and the date  
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

  

-- Remove the existing pros key  and btch date files

-- First get batch key and the date that we just inserted into UTIL_BTCH_ISAC 

USING 
( 
  FILLER CHAR(24),
  BTCHKEY CHAR(10)) 
  SELECT  CAST(CAST(BTCH_KEY_I AS INTEGER) AS CHAR(10))
         ,CAST(BTCH_RUN_D AS CHAR(10))
        
    FROM {{ bteq_var("VTECH") }}.UTIL_BTCH_ISAC
  WHERE BTCH_KEY_I = :BTCHKEY;

 

 
 
-- Using batch key and the date, obtain pros key


USING 

(  FILLER CHAR(4)
   ,BTCHKEY CHAR(10)
   ,EXTR_D CHAR(10)
   )
CALL {{ bteq_var("STARMACRDB") }}.SP_GET_PROS_KEY(        
  '{{ bteq_var("GDW_USER") }}',           -- From the config.pass parameters
  '{{ bteq_var("SRCE_SYST_M") }}',          
  '{{ bteq_var("SRCE_M") }}',           
  '{{ bteq_var("PSST_TABLE_M") }}',
  CAST(TRIM(:BTCHKEY) as DECIMAL(10,0)),
  CAST(:EXTR_D AS DATE) (FORMAT'YYYYMMDD')(CHAR(8)),            
  '{{ bteq_var("RSTR_F") }}',                -- Restart Flag
  'TD',                     
  IPROCESSKEY (CHAR(100))
);  

                		


 
