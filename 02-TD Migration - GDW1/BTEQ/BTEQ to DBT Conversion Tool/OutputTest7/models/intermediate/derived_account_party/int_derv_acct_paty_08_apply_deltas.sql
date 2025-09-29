-- =====================================================================
-- DBT Model: DERV_ACCT_PATY_08_APPLY_DELTAS
-- Converted from BTEQ: DERV_ACCT_PATY_08_APPLY_DELTAS.sql
-- Category: derived_account_party
-- Original Size: 7.5KB, 228 lines
-- Complexity Score: 116
-- Generated: 2025-08-21 13:48:49
-- =====================================================================

{{ intermediate_model_config() }}




------------------------------------------------------------------------------
-- Object Name             :  DERV_ACCT_PATY_08_APPLY_DELTAS.sql
-- Object Type             :  BTEQ

--                           
-- Description             :  Apply the changes as determined in previous step

------------------------------------------------------------------------------
-- Modification History
-- Date               Author           Version     Version Description
-- 04/06/2013         Helen Zak        1.0         Initial Version
-- 31/07/2013         Megan Disch      1.1         Pros key starts @ posn 4
-- 08/08/2013         Helen Zak        1.2         C0714578 - post-implementation fix
--                                                 1) read correct file (remove {{ bteq_var("TBSHORT") }} as it's not used)
--                                                 2) use PROS_KEY to read the row from UTIL_PROS_ISAC to obtain 
--                                                    BTCH_RUN_D, write both into a file
--                                                 3) import this file and use the values for applying deltas
-- 15/08/2013         Helen Zak        1.3         C0726912 - post-implementation fix
--                                                 Correct setting EFFT_D and EXPY_D for adds and updates.
--                                                 Logically -- Original DELETE removed: delete (by updating EXPY_D) rows that are no longer effective 
-- 28/08/2013         Helen Zak        1.4         C0733426 
--                                                 Use EFFT_D when applying deletions
-- 11/09/2013         Helen Zak      1.5           C0746488
--                                                Syncronise values of ROW_SECU_ACCS_C in DERV_ACCT_PATY with the values
--                                                for current rows in ACCT_PATY.              
------------------------------------------------------------------------------

-- Remove the existing pros key date file

--1. First get pros key and the date that we just inserted into UTIL_PROS_ISAC 

USING 
( 
  FILLER CHAR(4),
  PROSKEY CHAR(10) )
  SELECT  CAST(CAST(PROS_KEY_I AS INTEGER) AS CHAR(10))
         ,CAST(BTCH_RUN_D AS CHAR(10))
        
    FROM {{ bteq_var("VTECH") }}.UTIL_PROS_ISAC
  WHERE PROS_KEY_I = CAST(trim(:PROSKEY) as DECIMAL(10,0));
-- DBT handles table replacement via materialization strategy

 

 
-- 2. Update  rows from {{ bteq_var("STARDATADB") }}.DERV_ACCT_PATY that are no longer effective (logically delete)

USING 

(  FILLER CHAR(4)
   ,PROSKEY CHAR(10)
   ,EXTR_D CHAR(10)
   )
UPDATE T1
FROM   {{ bteq_var("STARDATADB") }}.DERV_ACCT_PATY T1
       ,{{ bteq_var("DDSTG") }}.DERV_ACCT_PATY_DEL T2
    SET    EXPY_D = CAST(:EXTR_D AS DATE) -1 (FORMAT'YYYY-MM-DD')(CHAR(10))
          ,PROS_KEY_EXPY_I = CAST(trim(:PROSKEY) as INTEGER)

  WHERE T1.ACCT_I = T2.ACCT_I
    AND T1.PATY_I = T2.PATY_I
    AND T1.PATY_ACCT_REL_C = T2.PATY_ACCT_REL_C
    AND T1.EFFT_D = T2.EFFT_D
    AND :EXTR_D BETWEEN T1.EFFT_D AND T1.EXPY_D
    AND :EXTR_D BETWEEN T2.EFFT_D AND T2.EXPY_D
  ;
   
 
--2. expire current rows that changed

USING 

(  FILLER CHAR(4)
   ,PROSKEY CHAR(10)
   ,EXTR_D CHAR(10)
   )
UPDATE T1
FROM {{ bteq_var("STARDATADB") }}.DERV_ACCT_PATY T1,
      (sel * from  PDDSTG.DERV_ACCT_PATY_CHG  qualify row_number() over (partition by ACCT_I,PATY_I,PATY_ACCT_REL_C order by efft_d desc)=1)T2

SET EXPY_D = CAST(:EXTR_D AS DATE) -1 (FORMAT'YYYY-MM-DD')(CHAR(10))
    ,PROS_KEY_EXPY_I = CAST(trim(:PROSKEY) as INTEGER)

WHERE T1.ACCT_I = T2.ACCT_I
AND T1.PATY_I = T2.PATY_I
AND T1.PATY_ACCT_REL_C = T2.PATY_ACCT_REL_C
AND (T1.ASSC_ACCT_I <> T2.ASSC_ACCT_I
  OR T1.PRFR_PATY_F <> T2.PRFR_PATY_F
  OR T1.SRCE_SYST_C <> T2.SRCE_SYST_C
  )
AND :EXTR_D BETWEEN T1.EFFT_D AND T1.EXPY_D
AND :EXTR_D BETWEEN T2.EFFT_D AND T2.EXPY_D;
  

-- 3. Insert new rows for the changes if they don't already exist (to cater for incorrect
--    history rows)

USING 

(  FILLER CHAR(4)
   ,PROSKEY CHAR(10)
   ,EXTR_D CHAR(10)
   )  
INSERT INTO {{ bteq_var("STARDATADB") }}.DERV_ACCT_PATY  
SELECT T1.ACCT_I
      
      ,T1.PATY_I
      ,T1.ASSC_ACCT_I
      ,T1.PATY_ACCT_REL_C
      ,T1.PRFR_PATY_F
      ,T1.SRCE_SYST_C
      ,:EXTR_D AS EFFT_D
      ,T1.EXPY_D
      ,CAST(trim(:PROSKEY) as INTEGER) AS PROS_KEY_EFFT_D
      ,CASE
          WHEN T1.EXPY_D = T1.EFFT_D THEN CAST(trim(:PROSKEY) as INTEGER)
          ELSE 0
       END AS PROS_KEY_EXPY_D
      ,T1.ROW_SECU_ACCS_C
       FROM {{ bteq_var("DDSTG") }}.DERV_ACCT_PATY_CHG T1
       
       LEFT JOIN {{ bteq_var("VTECH") }}.DERV_ACCT_PATY T2
       ON T1.ACCT_I = T2.ACCT_I
       AND T1.PATY_I = T2.PATY_I
       AND T1.PATY_aCCT_REL_C = T2.PATY_ACCT_REL_C
       AND T1.ASSC_ACCT_I = T2.ASSC_ACCT_I
       AND T1.SRCE_SYST_C = T2.SRCE_SYST_c
       AND T1.PRFR_PATY_f = T2.PRFR_PATY_f
       AND :EXTR_D BETWEEN t2.EFFT_D AND t2.EXPY_D
  
     WHERE :EXTR_D BETWEEN T1.EFFT_D AND T1.EXPY_D
       AND T2.ACCT_I IS NULL
       ;
 
 
  
--4. Insert rows into {{ bteq_var("STARDATADB") }}.DERV_ACCT_PATY that exist now but didn't exist before 


USING 

(  FILLER CHAR(4)
   ,PROSKEY CHAR(10)
   ,EXTR_D CHAR(10)
   )
-- Original INSERT converted to SELECT for DBT intermediate model
SELECT T1.ACCT_I
      ,T1.PATY_I
      ,T1.ASSC_ACCT_I
      ,T1.PATY_ACCT_REL_C
      ,T1.PRFR_PATY_F
      ,T1.SRCE_SYST_C
      ,T1.EFFT_D
      ,T1.EXPY_D
      ,CAST(trim(:PROSKEY) as INTEGER) AS PROS_KEY_EFFT_D
      ,CASE
          WHEN T1.EXPY_D = T1.EFFT_D THEN CAST(trim(:PROSKEY) as INTEGER)
          ELSE 0
       END AS PROS_KEY_EXPY_D
      ,T1.ROW_SECU_ACCS_C
FROM {{ bteq_var("DDSTG") }}.DERV_ACCT_PATY_ADD T1

  GROUP BY 1,2,3,4,5,6,7,8,9,10,11; 
     
  
--5. Identify accounts in {{ bteq_var("STARDATADB") }}.DERV_ACCT_PATY that have ROW_SECU_ACCS_C that is different from 
--    the current rows in ACCT_PATY and update them with the value from ACCT_PATY 

-- Original -- Original DELETE removed: DELETE removed: DELETE FROM  {{ bteq_var("DDSTG") }}.DERV_ACCT_PATY_ROW_SECU_FIX ALL;
-- DBT handles table replacement via materialization strategy
-- DBT handles table replacement via materialization strategy
  

INSERT INTO {{ bteq_var("DDSTG") }}.DERV_ACCT_PATY_ROW_SECU_FIX
SELECT T1.ACCT_I
, T1.ROW_SECU_ACCS_C AS DERV_ACCT_PATY_ROW_SECU_ACCS_C
, T2.ROW_SECU_ACCS_C AS ACCT_PATY_ROW_SECU_ACCS_C 
FROM {{ bteq_var("STARDATADB") }}.DERV_ACCT_PATY T1
JOIN {{ bteq_var("DDSTG") }}.DERV_ACCT_PATY_FLAG T2

ON T1.ACCT_I = T2.ACCT_I
AND T1.PATY_I=  T2.PATY_I
AND T1.ROW_SECU_ACCS_C <>  T2.ROW_SECU_ACCS_C
 
 GROUP BY 1,2,3;
   
 COLLECT STATS {{ bteq_var("DDSTG") }}.DERV_ACCT_PATY_ROW_SECU_FIX;
   
 UPDATE T1
FROM {{ bteq_var("STARDATADB") }}.DERV_ACCT_PATY T1,
{{ bteq_var("DDSTG") }}.DERV_ACCT_PATY_ROW_SECU_FIX T2
 
 SET ROW_SECU_ACCS_C = T2.ACCT_PATY_ROW_SECU_ACCS_C
 WHERE T1.ACCT_I = T2.ACCT_I
 AND T1.ROW_SECU_ACCS_C = T2.DERV_ACCT_PATY_ROW_SECU_ACCS_C;
   
 

-- $LastChangedBy: zakhe $
-- $LastChangedDate: 2013-09-11 15:36:32 +1000 (Wed, 11 Sep 2013) $
-- $LastChangedRevision: 12624 $

