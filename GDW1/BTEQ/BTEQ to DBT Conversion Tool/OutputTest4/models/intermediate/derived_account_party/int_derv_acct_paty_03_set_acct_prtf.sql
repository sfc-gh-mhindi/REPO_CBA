-- =====================================================================
-- DBT Model: DERV_ACCT_PATY_03_SET_ACCT_PRTF
-- Converted from BTEQ: DERV_ACCT_PATY_03_SET_ACCT_PRTF.sql
-- Category: derived_account_party
-- Original Size: 3.0KB, 87 lines
-- Complexity Score: 61
-- Generated: 2025-08-21 11:23:07
-- =====================================================================

{{ intermediate_model_config() }}


.IF ERRORLEVEL <> 0 THEN 

------------------------------------------------------------------------------
-- Object Name             :  DERV_ACCT_PATY_03_SET_ACCT_PRTF.sql
-- Object Type             :  BTEQ

--                           
-- Description             :  Get accounts that are relationship managed and ONLY ONE
--                            of the parties oin this account is relationship managed 
--                            by the same RM. This party will be a preferred party for
--                            such an account.   
--                          
------------------------------------------------------------------------------
-- Modification History
-- Date               Author           Version     Version Description
-- 04/06/2013         Helen Zak        1.0         Initial Version
--07/08/2013          Helen Zak        1.1         C0714578 - post-implementation fix
--                                                 Use persisted GRD table for better performance 
--                                                 Only get existing rows that have the flag set to 'N'
--                                                 as otherwise they don't need to change 
 -- 21/08/2013       Helen Zak       1.2          C0726912  - post-implementation fix
 --                                                                         Remove logic of getting existing rows
 --                                                                         as it should be handled by the delta process (in theory)
------------------------------------------------------------------------------

-- Get account portfolio details as per the extract date


DELETE FROM {{ bteq_var("DDSTG") }}.DERV_PRTF_ACCT_STAG;
-- {{ copy_from_stage("/cba_app/CBMGDW/{{ bteq_var("ENV_C") }}/schedule/{{ bteq_var("STRM_C") }}_extr_date.txt", "target_table") }}
USING 
( EXTR_D VARCHAR(10) )
-- Original INSERT converted to SELECT for DBT intermediate model
SELECT ACCT_I
        ,PRTF_CODE_X
   FROM  {{ bteq_var("VTECH") }}.DERV_PRTF_ACCT 
 
 WHERE PERD_D = :EXTR_D
   AND DERV_PRTF_CATG_C = 'RM'
    
 GROUP BY 1,2;
 

COLLECT STATS {{ bteq_var("DDSTG") }}.DERV_PRTF_ACCT_STAG;

-- Get party portfolio details as per the extract date

DELETE FROM {{ bteq_var("DDSTG") }}.DERV_PRTF_PATY_STAG;

-- {{ copy_from_stage("/cba_app/CBMGDW/{{ bteq_var("ENV_C") }}/schedule/{{ bteq_var("STRM_C") }}_extr_date.txt", "target_table") }}
USING 
( EXTR_D VARCHAR(10) )
-- Original INSERT converted to SELECT for DBT intermediate model
SELECT PATY_I
        ,PRTF_CODE_X
   FROM  {{ bteq_var("VTECH") }}.DERV_PRTF_PATY 
 
 WHERE PERD_D = :EXTR_D
   AND DERV_PRTF_CATG_C = 'RM'
 
 GROUP BY 1,2;

COLLECT STATS {{ bteq_var("DDSTG") }}.DERV_PRTF_PATY_STAG;


 
.QUIT 0

-- $LastChangedBy: zakhe $
-- $LastChangedDate: 2013-08-22 12:10:19 +1000 (Thu, 22 Aug 2013) $
-- $LastChangedRevision: 12529 $

.LABEL EXITERR
.QUIT 4
