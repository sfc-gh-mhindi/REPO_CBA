-- =====================================================================
-- DBT Model: DERV_ACCT_PATY_07_CRAT_DELTAS
-- Converted from BTEQ: DERV_ACCT_PATY_07_CRAT_DELTAS.sql
-- Category: derived_account_party
-- Original Size: 4.6KB, 152 lines
-- Complexity Score: 90
-- Generated: 2025-08-21 16:26:38
-- =====================================================================

{{
    config(
        materialized='table',
        docs={'show': true}
    )
}}




------------------------------------------------------------------------------
-- Object Name             :  DERV_ACCT_PATY_07_CRAT_DELTAS.sql
-- Object Type             :  BTEQ

--                           
-- Description             :  determine what changed in the table and apply the changes

------------------------------------------------------------------------------
-- Modification History
-- Date               Author           Version     Version Description
-- 04/06/2013         Helen Zak        1.0         Initial Version
-- 18/08/2013         Helen Zak        1.1         C0726912 - post-implementation fix
--                                                 Correct delta processing  - check for the rows
--                                                 that were effective before but not in the _FLAG
--                                                 table now  
-- 03/09/2013         Helen Zak        1.2         C0730261
--                                                 table DERV_ACCT_PATY_FLAG now contains current
--                                                 rows of interest (so that the original 
--                                                 table DERV)ACCT_PATY_CURR is preserved).
--                                                 This is done so that it is easier to rerun
    
------------------------------------------------------------------------------


--1. Insert rows into PDDSTG.DERV_ACCT_PATY_ADD that exist now but didn't exist before 

-- Original -- Original DELETE removed: DELETE removed: DELETE FROM {{ bteq_var("DDSTG") }}.DERV_ACCT_PATY_ADD;
-- DBT handles table replacement via materialization strategy
-- DBT handles table replacement via materialization strategy
 

USING 
( EXTR_D VARCHAR(10) ) 
-- Original INSERT converted to SELECT for DBT intermediate model
SELECT T1.ACCT_I
      ,T1.PATY_I
      ,T1.ASSC_ACCT_I
      ,T1.PATY_ACCT_REL_C
      ,T1.PRFR_PATY_F
      ,T1.SRCE_SYST_C
      ,T1.EFFT_D
      ,T1.EXPY_D
      ,T1.ROW_SECU_ACCS_C
FROM {{ bteq_var("DDSTG") }}.DERV_ACCT_PATY_FLAG T1
LEFT JOIN {{ bteq_var("VTECH") }}.DERV_ACCT_PATY T2

   ON T1.ACCT_I = T2.ACCT_I
  AND T1.PATY_I = T2.PATY_I
  AND T1.PATY_ACCT_REL_C = T2.PATY_ACCT_REL_C
  AND :EXTR_D BETWEEN T2.EFFT_D AND T2.EXPY_D
  
  WHERE :EXTR_D BETWEEN T1.EFFT_D AND T1.EXPY_D 
    AND T2.ACCT_I IS NULL
  GROUP BY 1,2,3,4,5,6,7,8,9; 
     
    
  COLLECT STATS {{ bteq_var("DDSTG") }}.DERV_ACCT_PATY_ADD; 
 

-- 2.Insert rows into PDDSTG.DERV_ACCT_PATY_CHG that changed 

-- Original -- Original DELETE removed: DELETE removed: DELETE FROM {{ bteq_var("DDSTG") }}.DERV_ACCT_PATY_CHG;
-- DBT handles table replacement via materialization strategy
-- DBT handles table replacement via materialization strategy

USING 
( EXTR_D VARCHAR(10) ) 
-- Original INSERT converted to SELECT for DBT intermediate model
SELECT T1.ACCT_I
      ,T1.PATY_I
      ,T1.ASSC_ACCT_I
      ,T1.PATY_ACCT_REL_C
      ,T1.PRFR_PATY_F
      ,T1.SRCE_SYST_C
      ,T1.EFFT_D
      ,T1.EXPY_D
      ,T1.ROW_SECU_ACCS_C
FROM {{ bteq_var("DDSTG") }}.DERV_ACCT_PATY_FLAG T1
JOIN {{ bteq_var("VTECH") }}.DERV_ACCT_PATY T2

  ON T1.ACCT_I = T2.ACCT_I
  AND T1.PATY_I = T2.PATY_I
  AND T1.PATY_ACCT_REL_C = T2.PATY_ACCT_REL_C
 
  WHERE (T1.ASSC_ACCT_I <> T2.ASSC_ACCT_I
  OR T1.PRFR_PATY_F <> T2.PRFR_PATY_F
  OR T1.SRCE_SYST_C <> T2.SRCE_SYST_C
  )
  AND :EXTR_D BETWEEN T1.EFFT_D AND T1.EXPY_D
  AND :EXTR_D BETWEEN T2.EFFT_D AND T2.EXPY_D
   GROUP BY 1,2,3,4,5,6,7,8,9;
  
 
 COLLECT STATS {{ bteq_var("DDSTG") }}.DERV_ACCT_PATY_CHG; 
 

--3. insert rows into PDDSTG.DERV_ACCT_PATY_DEL that were effective before but not in the current table any more 

-- Original -- Original DELETE removed: DELETE removed: DELETE FROM {{ bteq_var("DDSTG") }}.DERV_ACCT_PATY_DEL;
-- DBT handles table replacement via materialization strategy
-- DBT handles table replacement via materialization strategy
 
USING 
( EXTR_D VARCHAR(10) )  
-- Original INSERT converted to SELECT for DBT intermediate model
SELECT T1.ACCT_I
      ,T1.PATY_I
      ,T1.ASSC_ACCT_I
      ,T1.PATY_ACCT_REL_C
      ,T1.PRFR_PATY_F
      ,T1.SRCE_SYST_C
      ,T1.EFFT_D
      ,T1.EXPY_D
      ,T1.ROW_SECU_ACCS_C
FROM {{ bteq_var("VTECH") }}.DERV_ACCT_PATY T1
LEFT JOIN {{ bteq_var("DDSTG") }}.DERV_ACCT_PATY_FLAG T2

  ON T1.ACCT_I = T2.ACCT_I
  AND T1.PATY_I = T2.PATY_I
  AND T1.PATY_ACCT_REL_C = T2.PATY_ACCT_REL_C
  AND :EXTR_D BETWEEN T2.EFFT_D AND T2.EXPY_D
 
  WHERE :EXTR_D BETWEEN T1.EFFT_D AND T1.EXPY_D 
    AND T2.ACCT_I IS NULL 
  GROUP BY 1,2,3,4,5,6,7,8,9; 
     
    
  COLLECT STATS {{ bteq_var("DDSTG") }}.DERV_ACCT_PATY_DEL; 
   

-- $LastChangedBy: zakhe $
-- $LastChangedDate: 2013-09-04 10:43:45 +1000 (Wed, 04 Sep 2013) $
-- $LastChangedRevision: 12585 $

