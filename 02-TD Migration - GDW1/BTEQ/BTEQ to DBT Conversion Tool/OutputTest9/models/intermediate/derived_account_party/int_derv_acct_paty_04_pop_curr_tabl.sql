-- =====================================================================
-- DBT Model: DERV_ACCT_PATY_04_POP_CURR_TABL
-- Converted from BTEQ: DERV_ACCT_PATY_04_POP_CURR_TABL.sql
-- Category: derived_account_party
-- Original Size: 42.3KB, 1254 lines
-- Complexity Score: 433
-- Generated: 2025-08-21 14:21:55
-- =====================================================================

{{ intermediate_model_config() }}




------------------------------------------------------------------------------
-- Object Name             :  DERV_ACCT_PATY_04_POP_CURR_TABL.sql
-- Object Type             :  BTEQ

--                           
-- Description             :  Populate staging table with the rows from all sources
--                            with the rows effective on extract date
--                          
------------------------------------------------------------------------------
-- Modification History
-- Date               Author           Version     Version Description
-- 04/06/2013         Helen Zak        1.0         Initial Version
-- 31/07/2014         Megan Disch      1.1         Flip MID, MTX and LMS ACCT_UNID_PATY ACCT_I and ASSC_ACCT_I
-- 07/08/2013         Helen Zak        1.2         Collect stats after the last insert of MOS accounts
--                                                 Include error processing where it's missing
-- 21/08/2013         Helen Zak        1.3         C0726912 - post-implementation fix
--                                                 Get all effective rows from all sources
-- 06/11/2013         Helen Zak       1.4         C0800027  R32 Nexus
--                                                                          Include additional join condition for LMS, MID and MTX accounts
-- 21/07/2016   A&IPlatformRRTSquad@cba.com.au 
--				       1.5         C2151903  WSS :Change Internal deal paty_i from External Deal to Internal Deal. 
--            


------------------------------------------------------------------------------


 
-- insert into DERV_PATY_ALL_CURR from all available sources
-- but only rows that were effective on the extract date.
-- If a row expired ON or AFTER extract date, push its expiry date to '9999-12-31'
-- so that an account looks like it did on the extract date.
-- The only rows that expired on extract date that are included are those with
-- effective date being the same as expiry date

--1. everything from ACCT_PATY that was effective
-- on that date. Remove duplicates from ACCT_PATY first

-- Original -- Original DELETE removed: DELETE removed: DELETE FROM {{ bteq_var("DDSTG") }}.ACCT_PATY_DEDUP;
-- DBT handles table replacement via materialization strategy
-- DBT handles table replacement via materialization strategy

COLLECT STATS {{ bteq_var("DDSTG") }}.ACCT_PATY_DEDUP;

USING 
( EXTR_D VARCHAR(10) )
INSERT INTO {{ bteq_var("DDSTG") }}.ACCT_PATY_DEDUP
SELECT AP.ACCT_I
,PATY_I
,AP.ACCT_I AS ASSC_ACCT_I
,PATY_ACCT_REL_C
,'N'
,SRCE_SYST_C
,EFFT_D
,CASE
        WHEN EFFT_D = EXPY_D THEN EXPY_D
        WHEN EXPY_D >= :EXTR_D THEN '9999-12-31'::DATE
        ELSE EXPY_D
  END AS EXPY_D      
,AP.ROW_SECU_ACCS_C

FROM {{ bteq_var("VTECH") }}.ACCT_PATY AP

WHERE :EXTR_D BETWEEN AP.EFFT_D AND AP.EXPY_D  
QUALIFY ROW_NUMBER() OVER (PARTITION BY ACCT_I, PATY_I, PATY_ACCT_REL_C ORDER BY EFFT_D) = 1

;



COLLECT STATS {{ bteq_var("DDSTG") }}.ACCT_PATY_DEDUP;

--Now insert into DERV_ACCT_PATY_CURR rows from ACCT_PATY without duplicates
-- and use this table for other extracts as well

-- Original -- Original DELETE removed: DELETE removed: DELETE FROM {{ bteq_var("DDSTG") }}.DERV_ACCT_PATY_CURR;
-- DBT handles table replacement via materialization strategy
-- DBT handles table replacement via materialization strategy

COLLECT STATS {{ bteq_var("DDSTG") }}.DERV_ACCT_PATY_CURR;

USING 
( EXTR_D VARCHAR(10) )
INSERT INTO {{ bteq_var("DDSTG") }}.DERV_ACCT_PATY_CURR
SELECT ACCT_I
,PATY_I
,ASSC_ACCT_I
,PATY_ACCT_REL_C
,PRFR_PATY_F
,SRCE_SYST_C
,EFFT_D
,EXPY_D      
,ROW_SECU_ACCS_C

FROM {{ bteq_var("DDSTG") }}.ACCT_PATY_DEDUP

;




COLLECT STATS {{ bteq_var("DDSTG") }}.DERV_ACCT_PATY_CURR;

 
  
-- 2. BPS accounts - convert using ACCT_REL and XREF table for these accounts
--Select only the unique intersected (maximum of the EFFT_Ds and minimum of the EXPY_Ds) rows between the tables:
--Retrieve the intersection of date ranges between the entries from the three tables:
--1. Find the intersection between ACCT_PATY and ACCT_XREF_BPS_CBS.  Get the greater of the EFFT_D as the new EFFT_D.
--2. Find the intersection between the row created above with ACCT_REL.  Get the greater of the EFFT_D as the EFFT_D.
--3. Find the intersection between ACCT_PATY and ACCT_XREF_BPS_CBS.  Get the smaller of the EXPY_D as the new EXPY_D.
--4. Find the intersection between the row created above with ACCT_REL.  Get the smaller of the EXPY_D as the EXPY_D.

USING 
( EXTR_D VARCHAR(10) )
 -- Original INSERT converted to SELECT for DBT intermediate model
SELECT AX.BPS_ACCT_I AS ACCT_I
             ,AP.PATY_I
            ,CBS_ACCT_I AS ASSC_ACCT_I
            ,AP.PATY_ACCT_REL_C
            ,'N' AS PRFR_PATY_F  
            ,AP.SRCE_SYST_C
          ,(CASE WHEN AP.EFFT_D > AX.EFFT_D THEN AP.EFFT_D ELSE AX.EFFT_D END) AS EFFT_D
          ,(CASE WHEN AP.EXPY_D < AX.EXPY_D THEN AP.EXPY_D ELSE AX.EXPY_D END) AS EXPY_D
          ,AP.ROW_SECU_ACCS_C

 FROM (
                 SELECT ACCT_I
                               ,PATY_I
                               ,PATY_ACCT_REL_C
                               ,SRCE_SYST_C
                               ,EFFT_D
                               ,CASE
                                       WHEN EFFT_D = EXPY_D THEN EXPY_D
                                       WHEN EXPY_D >= :EXTR_D THEN '9999-12-31'::DATE
                                      ELSE EXPY_D
                                END AS EXPY_D      
                             ,ROW_SECU_ACCS_C
                 FROM   {{ bteq_var("DDSTG") }}.ACCT_PATY_DEDUP  
              WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D
          ) AP   
  JOIN (
              SELECT CBS_ACCT_I
                           ,BPS_ACCT_I
                           ,EFFT_D
                           ,CASE
                                       WHEN EFFT_D = EXPY_D THEN EXPY_D
                                       WHEN EXPY_D >= :EXTR_D THEN '9999-12-31'::DATE
                                      ELSE EXPY_D
                            END AS EXPY_D    
                FROM   {{ bteq_var("VTECH") }}.ACCT_XREF_BPS_CBS
              WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D
          )   AX 
ON AP.ACCT_I = AX.CBS_ACCT_I 
 WHERE (
                     (AP.EFFT_D BETWEEN AX.EFFT_D AND AX.EXPY_D) 
                OR (AX.EFFT_D BETWEEN AP.EFFT_D AND AP.EXPY_D)
             )
 
  
GROUP BY 1,2,3,4,5,6,7,8,9
              
UNION ALL

SELECT
  AR.OBJC_ACCT_I AS ACCT_I
, BPS.PATY_I
, BPS.CBS_ACCT_I AS ASSC_ACCT_I
, BPS.PATY_ACCT_REL_C
,'N' AS PRFR_PATY_F  
, BPS.SRCE_SYST_C
, (CASE WHEN AR.EFFT_D >  BPS.EFFT_D  THEN AR.EFFT_D  ELSE BPS.EFFT_D END) AS EFFT_D
, (CASE WHEN AR.EXPY_D <  BPS.EXPY_D THEN AR.EXPY_D ELSE BPS.EXPY_D END) AS EXPY_D
 ,BPS.ROW_SECU_ACCS_C

FROM  (SELECT SUBJ_ACCT_I
                      ,OBJC_ACCT_I
                      ,EFFT_D
                      ,CASE
                                WHEN EFFT_D = EXPY_D THEN EXPY_D
                                WHEN EXPY_D >= :EXTR_D THEN '9999-12-31'::DATE
                                 ELSE EXPY_D
                        END AS EXPY_D    
             FROM   {{ bteq_var("VTECH") }}.ACCT_REL  
           WHERE REL_C = 'FLBLL'
                AND :EXTR_D BETWEEN EFFT_D AND EXPY_D ) AR
JOIN

(
SELECT AX.BPS_ACCT_I
            ,CBS_ACCT_I 
            ,AP.PATY_I
            ,AP.PATY_ACCT_REL_C
            ,AP.SRCE_SYST_C
          ,(CASE WHEN AP.EFFT_D > AX.EFFT_D THEN AP.EFFT_D ELSE AX.EFFT_D END) AS EFFT_D
          ,(CASE WHEN AP.EXPY_D < AX.EXPY_D THEN AP.EXPY_D ELSE AX.EXPY_D END) AS EXPY_D
           ,AP.ROW_SECU_ACCS_C
 FROM (
                SELECT ACCT_I
                     ,PATY_I
                     ,PATY_ACCT_REL_C
                     ,SRCE_SYST_C
                     ,EFFT_D
                     ,CASE
                                WHEN EFFT_D = EXPY_D THEN EXPY_D
                                WHEN EXPY_D >= :EXTR_D THEN '9999-12-31'::DATE
                                 ELSE EXPY_D
                        END AS EXPY_D
                     ,ROW_SECU_ACCS_C       
           FROM    {{ bteq_var("DDSTG") }}.ACCT_PATY_DEDUP
             WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D)  AP 

JOIN  (
              SELECT CBS_ACCT_I
                           ,BPS_ACCT_I
                           ,EFFT_D
                           ,CASE
                                      WHEN EFFT_D = EXPY_D THEN EXPY_D
                                       WHEN EXPY_D >= :EXTR_D THEN '9999-12-31'::DATE
                                      ELSE EXPY_D
                            END AS EXPY_D    
                FROM   {{ bteq_var("VTECH") }}.ACCT_XREF_BPS_CBS
              WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D
          )   AX 
ON AP.ACCT_I = AX.CBS_ACCT_I 
WHERE  (
                     (AP.EFFT_D BETWEEN AX.EFFT_D AND AX.EXPY_D) 
                OR (AX.EFFT_D BETWEEN AP.EFFT_D AND AP.EXPY_D)
                   )


              
        GROUP BY 1, 2, 3, 4, 5,6,7,8
    
) BPS
ON AR.SUBJ_ACCT_I = BPS.BPS_ACCT_I 

WHERE (
                (AR.EFFT_D BETWEEN BPS.EFFT_D AND BPS.EXPY_D) OR 
                (BPS.EFFT_D BETWEEN AR.EFFT_D AND AR.EXPY_D)
              )
GROUP BY 1,2,3,4,5,6,7,8,9;



COLLECT STATS {{ bteq_var("DDSTG") }}.DERV_ACCT_PATY_CURR;

-- 3. CLS accounts - convert using CLS_FCLY and CLS_UNID_PATY
--Retrieve the intersection of date ranges between the entries from the three tables to derive new EFFT_D and EXPY_D:
--1. Find the intersection between CLS_FCLY and CLS_UNID_PATY.  Get the greater of the EFFT_D as the new EFFT_D.
--2. Find the intersection between the row created above with ACCT_PATY.  Get the greater of the EFFT_D as the EFFT_D.
--3. Find the intersection between CLS_FCLY and CLS_UNID_PATY.  Get the smaller of the EXPY_D as the new EXPY_D.
--4. Find the intersection between the row created above with ACCT_PATY.  Get the smaller of the EXPY_D as the EXPY_D.

USING 
( EXTR_D VARCHAR(10) )
INSERT INTO {{ bteq_var("DDSTG") }}.DERV_ACCT_PATY_CURR
 SELECT CLS.ACCT_I
      ,AP.PATY_I
     ,CLS.GDW_ACCT_I AS ASSC_ACCT_I 
     ,AP.PATY_ACCT_REL_C
     ,'N' AS PRFR_PATY_F  
     ,AP.SRCE_SYST_C
   , (CASE WHEN AP.EFFT_D > CLS.EFFT_D THEN AP.EFFT_D ELSE CLS.EFFT_D END) AS EFFT_D
   , (CASE WHEN AP.EXPY_D < CLS.EXPY_D THEN AP.EXPY_D ELSE CLS.EXPY_D END) AS EXPY_D
   ,AP.ROW_SECU_ACCS_C
FROM  (
                SELECT ACCT_I
                     ,PATY_I
                     ,PATY_ACCT_REL_C
                     ,SRCE_SYST_C
                     ,EFFT_D
                     ,CASE
                                WHEN EFFT_D = EXPY_D THEN EXPY_D
                                WHEN EXPY_D >= :EXTR_D THEN '9999-12-31'::DATE
                                 ELSE EXPY_D
                        END AS EXPY_D
                     ,ROW_SECU_ACCS_C       
           FROM    {{ bteq_var("DDSTG") }}.ACCT_PATY_DEDUP
             WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D)  AP 

JOIN 
    (SELECT CF.ACCT_I
    , 'CLSCO'||TRIM(CUP.CRIS_DEBT_I) AS GDW_ACCT_I
    ,(CASE WHEN CF.EFFT_D > CUP.EFFT_D THEN CF.EFFT_D ELSE CUP.EFFT_D END) AS EFFT_D
    ,(CASE WHEN CF.EXPY_D < CUP.EXPY_D THEN CF.EXPY_D ELSE CUP.EXPY_D END) AS EXPY_D
     FROM  (SELECT ACCT_I
                          ,SRCE_SYST_PATY_I
                         ,EFFT_D
                         ,CASE
                                WHEN EFFT_D = EXPY_D THEN EXPY_D
                                WHEN EXPY_D >= :EXTR_D THEN '9999-12-31'::DATE
                                 ELSE EXPY_D
                        END AS EXPY_D
             FROM {{ bteq_var("VTECH") }}.CLS_FCLY
             WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D)  CF

     JOIN  (
                   SELECT SRCE_SYST_PATY_I
                         ,CRIS_DEBT_I
                         ,EFFT_D
                         ,CASE
                                WHEN EFFT_D = EXPY_D THEN EXPY_D
                                WHEN EXPY_D >= :EXTR_D THEN '9999-12-31'::DATE
                                 ELSE EXPY_D
                        END AS EXPY_D
                  FROM {{ bteq_var("VTECH") }}.CLS_UNID_PATY
                  WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D) CUP 
     ON CUP.SRCE_SYST_PATY_I  = CF.SRCE_SYST_PATY_I 
     WHERE (
            (CUP.EFFT_D BETWEEN CF.EFFT_D AND CF.EXPY_D) 
        OR (CF.EFFT_D BETWEEN CUP.EFFT_D AND CUP.EXPY_D)
       )
 
        GROUP BY 1, 2, 3, 4
) AS CLS   
ON CLS.GDW_ACCT_I = AP.ACCT_I
WHERE  (
        (AP.EFFT_D BETWEEN CLS.EFFT_D AND CLS.EXPY_D) OR 
         (CLS.EFFT_D BETWEEN AP.EFFT_D AND AP.EXPY_D)
      )


GROUP BY 1,2,3,4,5,6,7,8,9
;
 

             
COLLECT STATS {{ bteq_var("DDSTG") }}.DERV_ACCT_PATY_CURR;

-- 4. MAS accounts - convert using ACCT_XREF_MAS_DAR and MAS_ACCT
--Select only the unique intersected (maximum of the EFFT_Ds and minimum of the EXPY_Ds) rows between the tables:
--Retrieve the intersection of date ranges between the entries from the three tables
--1. Find the intersection between MAS_ACCT and ACCT_XREF_MAS_DAR.  Get the greater of the EFFT_D as the new EFFT_D.
--2. Find the intersection between the row created above with ACCT_PATY.  Get the greater of the EFFT_D as the EFFT_D.
--3. Find the intersection between MAS_ACCT and ACCT_XREF_MAS_DAR.  Get the smaller of the EXPY_D as the new EXPY_D.
--4. Find the intersection between the row created above with ACCT_PATY.  Get the smaller of the EXPY_D as the EXPY_D.

USING 
( EXTR_D VARCHAR(10) )
-- Original INSERT converted to SELECT for DBT intermediate model
SELECT DAR.MERC_ACCT_I AS ACCT_I
           , AP.PATY_I
            ,DAR.MAS_MERC_ACCT_I
           , AP.PATY_ACCT_REL_C
           ,'N' AS PRFR_PATY_I
           , AP.SRCE_SYST_C
          , (CASE WHEN AP.EFFT_D >  DAR.EFFT_D  THEN AP.EFFT_D  ELSE DAR.EFFT_D END) AS EFFT_D
          , (CASE WHEN AP.EXPY_D <  DAR.EXPY_D THEN AP.EXPY_D ELSE DAR.EXPY_D END) AS EXPY_D
           ,AP.ROW_SECU_ACCS_C
FROM   (
                SELECT ACCT_I
                     ,PATY_I
                     ,PATY_ACCT_REL_C
                     ,SRCE_SYST_C
                     ,EFFT_D
                     ,CASE
                                WHEN EFFT_D = EXPY_D THEN EXPY_D
                                WHEN EXPY_D >= :EXTR_D THEN '9999-12-31'::DATE
                                 ELSE EXPY_D
                        END AS EXPY_D
                     ,ROW_SECU_ACCS_C       
           FROM    {{ bteq_var("DDSTG") }}.ACCT_PATY_DEDUP
             WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D)  AP 


JOIN (

SELECT DA.MERC_ACCT_I
             ,AX.MAS_MERC_ACCT_I
              ,(CASE WHEN DA.EFFT_D > AX.EFFT_D THEN DA.EFFT_D ELSE AX.EFFT_D END) AS EFFT_D
               ,(CASE WHEN DA.EXPY_D < AX.EXPY_D THEN DA.EXPY_D ELSE AX.EXPY_D END) AS EXPY_D
FROM 
(SELECT  MERC_ACCT_I
         ,EFFT_D
         ,CASE
                 WHEN EFFT_D = EXPY_D THEN EXPY_D
                 WHEN EXPY_D >= :EXTR_D THEN '9999-12-31'::DATE
                    ELSE EXPY_D
       END AS EXPY_D
   FROM {{ bteq_var("VTECH") }}.DAR_ACCT
   WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D) DA
JOIN (
SELECT MAS_MERC_ACCT_I
             ,DAR_ACCT_I
             ,EFFT_D
              ,CASE
                 WHEN EFFT_D = EXPY_D THEN EXPY_D
                 WHEN EXPY_D >= :EXTR_D THEN '9999-12-31'::DATE
                    ELSE EXPY_D
              END AS EXPY_D
     FROM        {{ bteq_var("VTECH") }}.ACCT_XREF_MAS_DAR
     WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D)  AX
  ON AX.DAR_ACCT_I = DA.MERC_ACCT_I
WHERE (
               (AX.EFFT_D BETWEEN DA.EFFT_D AND DA.EXPY_D) 
         OR (DA.EFFT_D BETWEEN AX.EFFT_D AND AX.EXPY_D)
              )
    
GROUP BY 1, 2,3,4

) AS DAR   
ON DAR.MAS_MERC_ACCT_I = AP.ACCT_I

WHERE (
               (AP.EFFT_D BETWEEN DAR.EFFT_D AND DAR.EXPY_D)
         OR (DAR.EFFT_D BETWEEN AP.EFFT_D AND AP.EXPY_D)
             );


COLLECT STATS {{ bteq_var("DDSTG") }}.DERV_ACCT_PATY_CURR;   

-- 5.  convert using ACCT_REL and SUBJ_ACCT_I. The values of REL_C are stored in GRD_GNRC_MAP

USING 
( EXTR_D VARCHAR(10) )
-- Original INSERT converted to SELECT for DBT intermediate model
SELECT AR.OBJC_ACCT_I AS ACCT_I
     , AP.PATY_I
     ,AR.SUBJ_ACCT_I AS ASSC_ACCT_I
    , AP.PATY_ACCT_REL_C
    ,'N' AS PRFR_PATY_F
    , AP.SRCE_SYST_C
, (CASE WHEN AR.EFFT_D >  AP.EFFT_D  THEN AR.EFFT_D  ELSE AP.EFFT_D END) AS EFFT_D
, (CASE WHEN AR.EXPY_D <  AP.EXPY_D THEN AR.EXPY_D ELSE AP.EXPY_D END) AS EXPY_D
 ,AP.ROW_SECU_ACCS_C
FROM  (
                SELECT ACCT_I
                     ,PATY_I
                     ,PATY_ACCT_REL_C
                     ,SRCE_SYST_C
                     ,EFFT_D
                     ,CASE
                               WHEN EFFT_D = EXPY_D THEN EXPY_D
                                WHEN EXPY_D >= :EXTR_D THEN '9999-12-31'::DATE
                                 ELSE EXPY_D
                        END AS EXPY_D
                     ,ROW_SECU_ACCS_C       
           FROM    {{ bteq_var("DDSTG") }}.ACCT_PATY_DEDUP
             WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D)  AP 

JOIN (
            SELECT AR1.SUBJ_ACCT_I
                          ,AR1.OBJC_ACCT_I
                          ,AR1.EFFT_D
                          ,CASE
                                WHEN AR1.EFFT_D = AR1.EXPY_D THEN AR1.EXPY_D
                                WHEN AR1.EXPY_D >= :EXTR_D THEN '9999-12-31'::DATE
                                 ELSE AR1.EXPY_D
                        END AS EXPY_D
                     FROM {{ bteq_var("VTECH") }}.ACCT_REL AR1
                        JOIN {{ bteq_var("DDSTG") }}.GRD_GNRC_MAP_DERV_PATY_REL GGM
                        ON AR1.REL_C = GGM.REL_C
                     AND GGM.ACCT_I_C = 'SUBJ'   
                     AND :EXTR_D BETWEEN AR1.EFFT_D AND AR1.EXPY_D ) AR
  ON AR.SUBJ_ACCT_I = AP.ACCT_I 
      
WHERE ( (AR.EFFT_D BETWEEN AP.EFFT_D AND AP.EXPY_D) 
        OR  (AP.EFFT_D BETWEEN AR.EFFT_D AND AR.EXPY_D) 
    )
    
   
  
GROUP BY 1,2,3,4,5,6,7,8,9;



COLLECT STATS {{ bteq_var("DDSTG") }}.DERV_ACCT_PATY_CURR;   

-- 5.  convert using ACCT_REL and OBJC_ACCT_I. The values of REL_C are stored in GRD_GNRC_MAP

USING 
( EXTR_D VARCHAR(10) )
-- Original INSERT converted to SELECT for DBT intermediate model
SELECT AR.SUBJ_ACCT_I AS ACCT_I
     , AP.PATY_I
     ,AR.OBJC_ACCT_I AS ASSC_ACCT_I
    , AP.PATY_ACCT_REL_C
    ,'N' AS PRFR_PATY_F
    , AP.SRCE_SYST_C
, (CASE WHEN AR.EFFT_D >  AP.EFFT_D  THEN AR.EFFT_D  ELSE AP.EFFT_D END) AS EFFT_D
, (CASE WHEN AR.EXPY_D <  AP.EXPY_D THEN AR.EXPY_D ELSE AP.EXPY_D END) AS EXPY_D
 ,AP.ROW_SECU_ACCS_C
FROM (
                SELECT ACCT_I
                     ,PATY_I
                     ,PATY_ACCT_REL_C
                     ,SRCE_SYST_C
                     ,EFFT_D
                     ,CASE
                               WHEN EFFT_D = EXPY_D THEN EXPY_D
                                WHEN EXPY_D >= :EXTR_D THEN '9999-12-31'::DATE
                                 ELSE EXPY_D
                        END AS EXPY_D
                     ,ROW_SECU_ACCS_C       
           FROM    {{ bteq_var("DDSTG") }}.ACCT_PATY_DEDUP
             WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D)  AP

JOIN (
            SELECT AR1.SUBJ_ACCT_I
                          ,AR1.OBJC_ACCT_I
                          ,AR1.EFFT_D
                          ,CASE
                                WHEN AR1.EFFT_D = AR1.EXPY_D THEN AR1.EXPY_D
                                WHEN AR1.EXPY_D >= :EXTR_D THEN '9999-12-31'::DATE
                                 ELSE AR1.EXPY_D
                        END AS EXPY_D
                     FROM {{ bteq_var("VTECH") }}.ACCT_REL AR1
                        JOIN {{ bteq_var("DDSTG") }}.GRD_GNRC_MAP_DERV_PATY_REL GGM
                        ON AR1.REL_C = GGM.REL_C
                     AND GGM.ACCT_I_C = 'OBJC'   
                     AND :EXTR_D BETWEEN AR1.EFFT_D AND AR1.EXPY_D ) AR
  ON AR.OBJC_ACCT_I = AP.ACCT_I 
      
WHERE ( (AR.EFFT_D BETWEEN AP.EFFT_D AND AP.EXPY_D) 
        OR  (AP.EFFT_D BETWEEN AR.EFFT_D AND AR.EXPY_D) 
    )
    
   
GROUP BY 1,2,3,4,5,6,7,8,9;



COLLECT STATS {{ bteq_var("DDSTG") }}.DERV_ACCT_PATY_CURR;  

--7. Process WSS accounts

DEL FROM {{ bteq_var("DDSTG") }}.ACCT_PATY_REL_WSS;

COLLECT STATS {{ bteq_var("DDSTG") }}.ACCT_PATY_REL_WSS;

USING 
( EXTR_D VARCHAR(10) )
-- Original INSERT converted to SELECT for DBT intermediate model
SELECT AR.SUBJ_ACCT_I AS ACCT_I
            , AP.PATY_I
             ,AR.OBJC_ACCT_I AS ASSC_ACCT_I
            , AP.PATY_ACCT_REL_C
            ,'N' AS PRFR_PATY_F
            , AP.SRCE_SYST_C
            , (CASE WHEN AR.EFFT_D >  AP.EFFT_D  THEN AR.EFFT_D  ELSE AP.EFFT_D END) AS EFFT_D
           , (CASE WHEN AR.EXPY_D <  AP.EXPY_D THEN AR.EXPY_D ELSE AP.EXPY_D END) AS EXPY_D
           ,AP.ROW_SECU_ACCS_C
           
 FROM (
                SELECT ACCT_I
                     ,PATY_I
                     ,PATY_ACCT_REL_C
                     ,SRCE_SYST_C
                     ,EFFT_D
                     ,CASE
                                WHEN EFFT_D = EXPY_D THEN EXPY_D
                                WHEN EXPY_D >= :EXTR_D THEN '9999-12-31'::DATE
                                 ELSE EXPY_D
                        END AS EXPY_D
                     ,ROW_SECU_ACCS_C       
           FROM    {{ bteq_var("DDSTG") }}.ACCT_PATY_DEDUP
             WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D)  AP
   JOIN  (
            SELECT AR1.SUBJ_ACCT_I
                          ,AR1.OBJC_ACCT_I
                          ,AR1.EFFT_D
                          ,CASE
                                WHEN AR1.EFFT_D = AR1.EXPY_D THEN AR1.EXPY_D
                                WHEN AR1.EXPY_D >= :EXTR_D THEN '9999-12-31'::DATE
                                 ELSE AR1.EXPY_D
                        END AS EXPY_D
                     FROM {{ bteq_var("VTECH") }}.ACCT_REL AR1
                      WHERE  AR1.REL_C = 'FCLYO'     
                     AND :EXTR_D BETWEEN AR1.EFFT_D AND AR1.EXPY_D ) AR 
     ON AR.OBJC_ACCT_I = AP.ACCT_I 
   
WHERE ( (AR.EFFT_D BETWEEN AP.EFFT_D AND AP.EXPY_D) 
        OR  (AP.EFFT_D BETWEEN AR.EFFT_D AND AR.EXPY_D) 
        );


         
COLLECT STATS {{ bteq_var("DDSTG") }}.ACCT_PATY_REL_WSS;  
          

--Get the accounts that  also exist in ACCT_REL with DITPS relationship   
           
-- Original -- Original DELETE removed: DELETE removed: DELETE FROM {{ bteq_var("DDSTG") }}.ACCT_REL_WSS_DITPS;
-- DBT handles table replacement via materialization strategy
-- DBT handles table replacement via materialization strategy

 USING 
( EXTR_D VARCHAR(10) )
 INSERT INTO {{ bteq_var("DDSTG") }}.ACCT_REL_WSS_DITPS
 SELECT APA.ACCT_I
  FROM {{ bteq_var("DDSTG") }}. ACCT_PATY_REL_WSS APA
     JOIN   (
            SELECT AR1.SUBJ_ACCT_I
                          ,AR1.OBJC_ACCT_I
                          ,AR1.EFFT_D
                          ,CASE
                                WHEN AR1.EFFT_D = AR1.EXPY_D THEN AR1.EXPY_D
                                WHEN AR1.EXPY_D >= :EXTR_D THEN '9999-12-31'::DATE
                                 ELSE AR1.EXPY_D
                        END AS EXPY_D
                     FROM {{ bteq_var("VTECH") }}.ACCT_REL AR1
                      WHERE  AR1.REL_C = 'DITPS'
                     AND :EXTR_D BETWEEN AR1.EFFT_D AND AR1.EXPY_D ) AR
        ON APA.ACCT_I = AR.OBJC_ACCT_I
       AND (  (APA.EFFT_D BETWEEN AR.EFFT_D AND AR.EXPY_D) 
       OR  (AR.EFFT_D BETWEEN APA.EFFT_D AND APA.EXPY_D))
 
        GROUP BY 1 ;
 

COLLECT STATS {{ bteq_var("DDSTG") }}.ACCT_REL_WSS_DITPS;

   ---- Original DELETE removed: Delete those accounts that also exist in ACCT_REL with DITPS relationship. 
              
DEL  APA
  FROM {{ bteq_var("DDSTG") }}. ACCT_PATY_REL_WSS APA
      , {{ bteq_var("DDSTG") }}.ACCT_REL_WSS_DITPS T1
 WHERE APA.ACCT_I = T1.ACCT_I;
-- DBT handles table replacement via materialization strategy


 COLLECT STATS {{ bteq_var("DDSTG") }}.ACCT_PATY_REL_WSS;

--Insert accounts that also exist in ACCT_REL with DITPS relationship, using 
--OBJC_ACCT_I. 
 
 USING 
( EXTR_D VARCHAR(10) )
  INSERT INTO {{ bteq_var("DDSTG") }}.ACCT_PATY_REL_WSS       

 SELECT REL.ACCT_I
      ,AP.PATY_I
     ,REL.ASSC_ACCT_I 
     ,AP.PATY_ACCT_REL_C
     ,'N' AS PRFR_PATY_F  
     ,AP.SRCE_SYST_C
   , (CASE WHEN AP.EFFT_D > REL.EFFT_D THEN AP.EFFT_D ELSE REL.EFFT_D END) AS EFFT_D
   , (CASE WHEN AP.EXPY_D < REL.EXPY_D THEN AP.EXPY_D ELSE REL.EXPY_D END) AS EXPY_D

,AP.ROW_SECU_ACCS_C

FROM  (
                SELECT ACCT_I
                     ,PATY_I
                     ,PATY_ACCT_REL_C
                     ,SRCE_SYST_C
                     ,EFFT_D
                     ,CASE
                                WHEN EFFT_D = EXPY_D THEN EXPY_D
                                WHEN EXPY_D >= :EXTR_D THEN '9999-12-31'::DATE
                                 ELSE EXPY_D
                        END AS EXPY_D
                     ,ROW_SECU_ACCS_C       
           FROM    {{ bteq_var("DDSTG") }}.ACCT_PATY_DEDUP
             WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D) AP
JOIN 
    (SELECT DITPS.OBJC_ACCT_I AS ACCT_I
        ,FCLYO.OBJC_ACCT_I AS ASSC_ACCT_I
    ,(CASE WHEN FCLYO.EFFT_D > DITPS.EFFT_D THEN FCLYO.EFFT_D ELSE DITPS.EFFT_D END) AS EFFT_D
    ,(CASE WHEN FCLYO.EXPY_D < DITPS.EXPY_D THEN FCLYO.EXPY_D ELSE DITPS.EXPY_D END) AS EXPY_D
     FROM  (
            SELECT SUBJ_ACCT_I
                          ,OBJC_ACCT_I
                          ,EFFT_D
                          ,CASE
                                WHEN EFFT_D = EXPY_D THEN EXPY_D
                                WHEN EXPY_D >= :EXTR_D THEN '9999-12-31'::DATE
                                 ELSE EXPY_D
                        END AS EXPY_D
                     FROM {{ bteq_var("VTECH") }}.ACCT_REL AR1
                      WHERE  REL_C = 'FCLYO'
                     AND :EXTR_D BETWEEN EFFT_D AND EXPY_D ) FCLYO

     JOIN     (
            SELECT SUBJ_ACCT_I
                          ,OBJC_ACCT_I
                          ,REL_C
                          ,EFFT_D
                          ,CASE
                                WHEN EFFT_D = EXPY_D THEN EXPY_D
                                WHEN EXPY_D >= :EXTR_D THEN '9999-12-31'::DATE
                                 ELSE EXPY_D
                        END AS EXPY_D
                     FROM {{ bteq_var("VTECH") }}.ACCT_REL AR1
                      WHERE  REL_C = 'DITPS'
                     AND :EXTR_D BETWEEN EFFT_D AND EXPY_D ) DITPS 
     ON FCLYO.SUBJ_ACCT_I = DITPS.OBJC_ACCT_I /* C2151903 - WSS DERV ACCT_PATY CHANGE */			 

     
     
     JOIN {{ bteq_var("DDSTG") }}.ACCT_REL_WSS_DITPS AR3
         ON DITPS.OBJC_ACCT_I = AR3.ACCT_I
         
     WHERE (
            (DITPS.EFFT_D BETWEEN FCLYO.EFFT_D AND FCLYO.EXPY_D) 
        OR (FCLYO.EFFT_D BETWEEN DITPS.EFFT_D AND DITPS.EXPY_D)
       ) 
 --this is to eliminate duplicate relationships that exist for some of DITPS 
   
       QUALIFY ROW_NUMBER() OVER(PARTITION BY DITPS.OBJC_ACCT_I, DITPS.REL_C ORDER BY DITPS.SUBJ_ACCT_I DESC) = 1
    
) AS REL   
ON REL.ASSC_ACCT_I = AP.ACCT_I

WHERE (
                (AP.EFFT_D BETWEEN REL.EFFT_D AND REL.EXPY_D) OR 
                (REL.EFFT_D BETWEEN AP.EFFT_D AND AP.EXPY_D)
              )

GROUP BY 1,2,3,4,5,6,7,8,9
;

COLLECT STATS {{ bteq_var("DDSTG") }}.ACCT_PATY_REL_WSS;   

--Insert WSS rows into the main table

-- Original INSERT converted to SELECT for DBT intermediate model
SELECT ACCT_I                        
,PATY_I                        
,ASSC_ACCT_I                   
,PATY_ACCT_REL_C               
,PRFR_PATY_F                   
,SRCE_SYST_C                   
,EFFT_D                        
,EXPY_D                        
,ROW_SECU_ACCS_C               
FROM {{ bteq_var("DDSTG") }}.ACCT_PATY_REL_WSS;

COLLECT STATS {{ bteq_var("DDSTG") }}.DERV_ACCT_PATY_CURR;
 
--8. THA accounts - convert using THA_ACCT
-- 1. Select only one trade account as per existing FPR logic 
--Select only the unique intersected (maximum of the EFFT_Ds and minimum of the EXPY_Ds) rows between the tables.  
--Retrieve the intersection of date ranges between the entries from the two tables:
--1. Find the intersection between ACCT_PATY and THA_ACCT.  
--2. Get the greater of the EFFT_D.
--3. Get the smaller of the EXPY_D.

DEL FROM {{ bteq_var("DDSTG") }}.ACCT_PATY_REL_THA;

COLLECT STATS {{ bteq_var("DDSTG") }}.ACCT_PATY_REL_THA;
 
USING 
( EXTR_D VARCHAR(10) )   
INSERT INTO {{ bteq_var("DDSTG") }}.ACCT_PATY_REL_THA
  
  -- first get the max  
    
     SELECT THA_ACCT_I
          ,TRAD_ACCT_I
          ,EFFT_D
          ,CASE
                   WHEN EFFT_D = EXPY_D THEN EXPY_D
                   WHEN EXPY_D >= :EXTR_D THEN '9999-12-31'::DATE
                    ELSE EXPY_D
              END AS EXPY_D
         
          FROM {{ bteq_var("VTECH") }}.THA_ACCT T1
          
       WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D
          QUALIFY ROW_NUMBER() OVER (PARTITION BY THA_ACCT_I, EFFT_D
 ORDER BY TRAD_ACCT_I, CSL_CLNT_I DESC) = 1;
 
COLLECT STATS {{ bteq_var("DDSTG") }}.ACCT_PATY_REL_THA;

DEL FROM {{ bteq_var("DDSTG") }}.ACCT_PATY_THA_NEW_RNGE;

COLLECT STATS {{ bteq_var("DDSTG") }}.ACCT_PATY_THA_NEW_RNGE;

INSERT INTO {{ bteq_var("DDSTG") }}.ACCT_PATY_THA_NEW_RNGE
SELECT THA_ACCT_I
     ,TRAD_ACCT_I
      ,EFFT_D
     ,EXPY_D
     ,CASE WHEN NEW_EXPY_D IS NULL THEN EXPY_D 
           WHEN NEW_EXPY_D <= EXPY_D AND NEW_EXPY_D > EFFT_D THEN NEW_EXPY_D - 1     
           ELSE EXPY_D
       END AS NEW_EXPY_D 
       FROM

(SELECT THA_ACCT_I
             ,TRAD_ACCT_I
             ,EFFT_D
             ,EXPY_D
             ,MIN(EFFT_D)  
             OVER(PARTITION BY THA_ACCT_I
                         ORDER BY EFFT_D, EXPY_D
                         ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING
                        ) AS NEW_EXPY_D
      FROM {{ bteq_var("DDSTG") }}.ACCT_PATY_REL_THA
   
     
    ) T;


COLLECT STATS {{ bteq_var("DDSTG") }}.ACCT_PATY_THA_NEW_RNGE;

   INSERT INTO {{ bteq_var("DDSTG") }}.ACCT_PATY_THA_NEW_RNGE
     SELECT T1.THA_ACCT_I
     ,T1.TRAD_ACCT_I
     ,T1.NEW_EXPY_D + 1 AS EFFT_D
    ,T1.EXPY_D
    ,T1.EXPY_D
    FROM {{ bteq_var("DDSTG") }}.ACCT_PATY_THA_NEW_RNGE T1
    LEFT JOIN {{ bteq_var("DDSTG") }}.ACCT_PATY_THA_NEW_RNGE T2
    
    ON T1.THA_ACCT_I = T2.THA_ACCT_I
    
    AND T1.NEW_EXPY_D + 1  = T2.EFFT_D
    WHERE T1.NEW_EXPY_D < T1.EXPY_D AND T1.NEW_EXPY_D > T1.EFFT_D
    AND T2.THA_ACCT_I IS NULL
    
    GROUP BY 1,2,3,4,5;



    
 COLLECT STATS {{ bteq_var("DDSTG") }}.ACCT_PATY_THA_NEW_RNGE;
    
 DEL FROM {{ bteq_var("DDSTG") }}.ACCT_PATY_REL_THA;
  
COLLECT STATS {{ bteq_var("DDSTG") }}.ACCT_PATY_REL_THA;
 
INSERT INTO {{ bteq_var("DDSTG") }}.ACCT_PATY_REL_THA
    
    SELECT THA_ACCT_I
          ,MAX(TRAD_ACCT_I)
          ,EFFT_D
          ,NEW_EXPY_D
          FROM  {{ bteq_var("DDSTG") }}.ACCT_PATY_THA_NEW_RNGE
 
          GROUP BY 1,3,4;
 
           


COLLECT STATS {{ bteq_var("DDSTG") }}.ACCT_PATY_REL_THA;

USING 
( EXTR_D VARCHAR(10) )
-- Original INSERT converted to SELECT for DBT intermediate model
SELECT ACCT_I
            , PATY_I
            , MAX(ASSC_ACCT_I)
            , PATY_aCCT_REL_C
            , PRFR_PATY_F
            , SRCE_SYST_C
            , EFFT_D
            , EXPY_D
             ,ROW_SECU_ACCS_C
            FROM (

SELECT TA.THA_ACCT_I AS ACCT_I
, AP.PATY_I
,TA.TRAD_ACCT_I AS ASSC_ACCT_I
, AP.PATY_ACCT_REL_C
,'N' AS PRFR_PATY_F
, AP.SRCE_SYST_C
, (CASE WHEN TA.EFFT_D >  AP.EFFT_D  THEN TA.EFFT_D  ELSE AP.EFFT_D END) AS EFFT_D
, (CASE WHEN TA.EXPY_D <  AP.EXPY_D THEN TA.EXPY_D ELSE AP.EXPY_D END) AS EXPY_D
 ,AP.ROW_SECU_ACCS_C
 FROM   (
                SELECT ACCT_I
                     ,PATY_I
                     ,PATY_ACCT_REL_C
                     ,SRCE_SYST_C
                     ,EFFT_D
                     ,CASE
                                WHEN EFFT_D = EXPY_D THEN EXPY_D
                                WHEN EXPY_D >= :EXTR_D THEN '9999-12-31'::DATE
                                 ELSE EXPY_D
                        END AS EXPY_D
                     ,ROW_SECU_ACCS_C       
           FROM    {{ bteq_var("DDSTG") }}.ACCT_PATY_DEDUP
             WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D)  AP

    JOIN {{ bteq_var("DDSTG") }}.ACCT_PATY_REL_THA TA
      ON TA.TRAD_ACCT_I = AP.ACCT_I 
  
 WHERE ( 
        (TA.EFFT_D BETWEEN AP.EFFT_D AND AP.EXPY_D) 
    OR  (AP.EFFT_D BETWEEN TA.EFFT_D AND TA.EXPY_D) 
        ) 
  
  
     ) T 
        
GROUP BY 1,2,4,5,6,7,8,9;


 COLLECT STATS {{ bteq_var("DDSTG") }}.DERV_ACCT_PATY_CURR; 

--9. Insert MID, MTX and LMS accounts, using ACCT_UNID_PATY transformation

--Select only the unique intersected (maxium of the EFFT_Ds and minimum of the EXPY_Ds) rows between the tables.  

--1. Retrieve the non SAP entries (entries generated from product systems) from ACCT_UNID_PATY XREF1.
--2. Retrieve the SAP entries (entries generated from SAP BP data) from ACCT_UNID_PATY XREF2.
--3. Join ACCT_UNID_PATY A and ACCT_UNID_PATY_B using SRCE_SYST_PATY_I.  
--4. Join the entries returned by above to ACCT_PATY using XREF2.ACCT_I.

USING 
( EXTR_D VARCHAR(10) )
-- Original INSERT converted to SELECT for DBT intermediate model
SELECT XREF.ACCT_I 
      ,AP.PATY_I
      ,XREF.ASSC_ACCT_I 
     ,AP.PATY_ACCT_REL_C
     ,'N' AS PRFR_PATY_F
     ,AP.SRCE_SYST_C
    
   ,(CASE WHEN AP.EFFT_D > XREF.EFFT_D THEN AP.EFFT_D ELSE XREF.EFFT_D END) AS EFFT_D
   ,(CASE WHEN AP.EXPY_D < XREF.EXPY_D THEN AP.EXPY_D ELSE XREF.EXPY_D END) AS EXPY_D
   ,AP.ROW_SECU_ACCS_C
FROM (
                SELECT ACCT_I
                     ,PATY_I
                     ,PATY_ACCT_REL_C
                     ,SRCE_SYST_C
                     ,EFFT_D
                     ,CASE
                               WHEN EFFT_D = EXPY_D THEN EXPY_D
                                WHEN EXPY_D >= :EXTR_D THEN '9999-12-31'::DATE
                                 ELSE EXPY_D
                        END AS EXPY_D
                     ,ROW_SECU_ACCS_C       
           FROM    {{ bteq_var("DDSTG") }}.ACCT_PATY_DEDUP
             WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D)  AP
JOIN (
SELECT XREF1.ACCT_I   
    ,XREF2. ASSC_ACCT_I
    ,(CASE WHEN XREF2.EFFT_D > XREF1.EFFT_D THEN XREF2.EFFT_D ELSE XREF1.EFFT_D END) AS EFFT_D
    ,(CASE WHEN XREF2.EXPY_D < XREF1.EXPY_D THEN XREF2.EXPY_D ELSE XREF1.EXPY_D END) AS EXPY_D

FROM (SELECT ACCT_I
                     ,SRCE_SYST_PATY_I
                     ,SRCE_SYST_C
                     ,EFFT_D
                     ,CASE
                                WHEN EFFT_D = EXPY_D THEN EXPY_D
                                WHEN EXPY_D >= :EXTR_D THEN '9999-12-31'::DATE
                                 ELSE EXPY_D
                        END AS EXPY_D

           FROM   {{ bteq_var("VTECH") }}.ACCT_UNID_PATY
           WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D)   XREF1
JOIN (SELECT ACCT_I AS ASSC_ACCT_I 
                  ,SRCE_SYST_PATY_I
                  ,PATY_ACCT_REL_C
                  ,SRCE_SYST_C
                  ,ORIG_SRCE_SYST_C         -- R32 change
                  ,EFFT_D
                     ,CASE
                               WHEN EFFT_D = EXPY_D THEN EXPY_D
                                WHEN EXPY_D >= :EXTR_D THEN '9999-12-31'::DATE
                                 ELSE EXPY_D
                        END AS EXPY_D

           FROM   {{ bteq_var("VTECH") }}.ACCT_UNID_PATY
           WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D)  XREF2 
  ON XREF2.SRCE_SYST_PATY_I = XREF1.SRCE_SYST_PATY_I 
  
JOIN {{ bteq_var("DDSTG") }}.GRD_GNRC_MAP_DERV_UNID_PATY GGM 
  ON GGM.UNID_PATY_SRCE_SYST_C = XREF2.SRCE_SYST_C
 AND GGM.UNID_PATY_ACCT_REL_C  = XREF2.PATY_ACCT_REL_C
  
   -- R32 change starts
 AND GGM.SRCE_SYST_C  = XREF2.ORIG_SRCE_SYST_C 
  -- R32 change ends
  
WHERE XREF1.SRCE_SYST_C = GGM.SRCE_SYST_C
AND  (
      (XREF1.EFFT_D BETWEEN XREF2.EFFT_D AND XREF2.EXPY_D) 
  OR  (XREF2.EFFT_D BETWEEN XREF1.EFFT_D AND XREF1.EXPY_D))
     ) XREF  
     
  ON XREF.ASSC_ACCT_I = AP.ACCT_I
WHERE  ((AP.EFFT_D BETWEEN XREF.EFFT_D AND XREF.EXPY_D) 
    OR  (XREF.EFFT_D BETWEEN AP.EFFT_D AND AP.EXPY_D)
    )



GROUP BY 1,2,3,4,5,6,7,8,9;



 COLLECT STATS {{ bteq_var("DDSTG") }}.DERV_ACCT_PATY_CURR; 

-- 10. Include MOS account level ACCT_Is Party mappings based on the corresponding MOS facilities to Party mapping records. 


--Select only the unique intersected (maxium of the EFFT_Ds and minimum of the EXPY_Ds) rows between the tables.  

-- Generate rows for loan accounts
--1. Find the facility accounts for the loan accounts.
--2. Find the ACCT_I in ACCT_UNID_PATY using the SRCE_SYST_PATY_I.
--3. Use the ACCT_I found for above to find PATY_I from ACCT_PATY table.

USING 
( EXTR_D VARCHAR(10) )
-- Original INSERT converted to SELECT for DBT intermediate model
SELECT AX.ACCT_I
       ,AP.PATY_I
       ,AX.ASSC_ACCT_I 
       ,AP.PATY_ACCT_REL_C
       ,'N' AS PRFR_PATY_F  
       ,AP.SRCE_SYST_C
       ,(CASE WHEN AP.EFFT_D > AX.EFFT_D THEN AP.EFFT_D ELSE AX.EFFT_D END) AS EFFT_D
       ,(CASE WHEN AP.EXPY_D < AX.EXPY_D THEN AP.EXPY_D ELSE AX.EXPY_D END) AS EXPY_D
       ,AP.ROW_SECU_ACCS_C
FROM (SELECT ACCT_I
          ,PATY_I
          ,PATY_ACCT_REL_C
          ,SRCE_SYST_C
           ,EFFT_D
           ,CASE
               WHEN EFFT_D = EXPY_D THEN EXPY_D
               WHEN EXPY_D >= :EXTR_D THEN '9999-12-31'::DATE
               ELSE EXPY_D
            END AS EXPY_D
            ,ROW_SECU_ACCS_C       
           FROM {{ bteq_var("DDSTG") }}.ACCT_PATY_DEDUP
       WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D) AP
JOIN
( --start AX
SELECT T1.LOAN_I AS ACCT_I
       ,UIP.ACCT_I AS ASSC_ACCT_I
        ,(CASE WHEN UIP.EFFT_D > T1.EFFT_D THEN UIP.EFFT_D ELSE T1.EFFT_D END) AS EFFT_D
       ,(CASE WHEN UIP.EXPY_D < T1.EXPY_D THEN UIP.EXPY_D ELSE T1.EXPY_D END) AS EXPY_D
       FROM    
(  --start T1
SELECT LOAN.LOAN_I 
       ,FCLY.SRCE_SYST_PATY_I
       ,(CASE WHEN LOAN.EFFT_D > FCLY.EFFT_D THEN LOAN.EFFT_D ELSE FCLY.EFFT_D END) AS EFFT_D
       ,(CASE WHEN LOAN.EXPY_D < FCLY.EXPY_D THEN LOAN.EXPY_D ELSE FCLY.EXPY_D END) AS EXPY_D 
FROM ( SELECT LOAN_I
           ,FCLY_I
           ,EFFT_D
            ,CASE
                WHEN EFFT_D = EXPY_D THEN EXPY_D
                WHEN EXPY_D >= :EXTR_D THEN '9999-12-31'::DATE
                ELSE EXPY_D
             END AS EXPY_D
      FROM {{ bteq_var("VTECH") }}.MOS_LOAN 
      WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D) LOAN 
JOIN (SELECT SUBSTR(FCLY_I,1,14) AS MOS_FCLY_I
         ,SRCE_SYST_PATY_I
         ,EFFT_D
           ,CASE
                WHEN EFFT_D = EXPY_D THEN EXPY_D
                WHEN EXPY_D >= :EXTR_D THEN '9999-12-31'::DATE
                ELSE EXPY_D
             END AS EXPY_D
         FROM {{ bteq_var("VTECH") }}.MOS_FCLY
        WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D) FCLY  
           ON FCLY.MOS_FCLY_I = LOAN.FCLY_I
       WHERE (
              (FCLY.EFFT_D BETWEEN LOAN.EFFT_D AND LOAN.EXPY_D) 
             OR (LOAN.EFFT_D BETWEEN FCLY.EFFT_D AND FCLY.EXPY_D)
            )
 ) T1           
JOIN (SELECT ACCT_I 
               ,SRCE_SYST_PATY_I
               ,EFFT_D
               ,CASE
                  WHEN EFFT_D = EXPY_D THEN EXPY_D
                  WHEN EXPY_D >= :EXTR_D THEN '9999-12-31'::DATE
                  ELSE EXPY_D
                END AS EXPY_D
           FROM {{ bteq_var("VTECH") }}.ACCT_UNID_PATY
           WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D
            AND SRCE_SYST_C = 'SAP' 
            AND PATY_ACCT_REL_C = 'ACTO') UIP 
ON UIP.SRCE_SYST_PATY_I = T1.SRCE_SYST_PATY_I 
WHERE (
       (UIP.EFFT_D BETWEEN T1.EFFT_D AND T1.EXPY_D) 
        OR (T1.EFFT_D BETWEEN UIP.EFFT_D AND UIP.EXPY_D)
      )
) AX  
ON AX.ASSC_ACCT_I = AP.ACCT_I

WHERE (
       (AX.EFFT_D BETWEEN AP.EFFT_D AND AP.EXPY_D) 
        OR (AP.EFFT_D BETWEEN AX.EFFT_D AND AX.EXPY_D)
      )



GROUP BY 1,2,3,4,5,6,7,8,9

UNION ALL



-- Generate rows for facility accounts

SELECT  AX.ACCT_I
       ,AP.PATY_I
       ,AX.ASSC_ACCT_I 
       ,AP.PATY_ACCT_REL_C
       ,'N' AS PRFR_PATY_F  
       ,AP.SRCE_SYST_C
       ,(CASE WHEN AP.EFFT_D > AX.EFFT_D THEN AP.EFFT_D ELSE AX.EFFT_D END) AS EFFT_D
       ,(CASE WHEN AP.EXPY_D < AX.EXPY_D THEN AP.EXPY_D ELSE AX.EXPY_D END) AS EXPY_D
       ,AP.ROW_SECU_ACCS_C
FROM (SELECT ACCT_I
         ,PATY_I
         ,PATY_ACCT_REL_C
         ,SRCE_SYST_C
         ,EFFT_D
         ,CASE
              WHEN EFFT_D = EXPY_D THEN EXPY_D
              WHEN EXPY_D >= :EXTR_D THEN '9999-12-31'::DATE
               ELSE EXPY_D
          END AS EXPY_D
          ,ROW_SECU_ACCS_C       
       FROM {{ bteq_var("DDSTG") }}.ACCT_PATY_DEDUP
       WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D) AP
JOIN 
(--start AX
SELECT FCLY.FCLY_I AS ACCT_I 
       ,UIP.ACCT_I AS ASSC_ACCT_I
       ,(CASE WHEN UIP.EFFT_D > FCLY.EFFT_D THEN UIP.EFFT_D ELSE FCLY.EFFT_D END) AS EFFT_D
       ,(CASE WHEN UIP.EXPY_D < FCLY.EXPY_D THEN UIP.EXPY_D ELSE FCLY.EXPY_D END) AS EXPY_D
FROM (SELECT FCLY_I
         ,SRCE_SYST_PATY_I
         ,EFFT_D
           ,CASE
                WHEN EFFT_D = EXPY_D THEN EXPY_D
                WHEN EXPY_D >= :EXTR_D THEN '9999-12-31'::DATE
                ELSE EXPY_D
             END AS EXPY_D
         FROM {{ bteq_var("VTECH") }}.MOS_FCLY
        WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D) FCLY  
JOIN (SELECT ACCT_I 
               ,SRCE_SYST_PATY_I
               ,EFFT_D
               ,CASE
                  WHEN EFFT_D = EXPY_D THEN EXPY_D
                  WHEN EXPY_D >= :EXTR_D THEN '9999-12-31'::DATE
                  ELSE EXPY_D
                END AS EXPY_D
           FROM {{ bteq_var("VTECH") }}.ACCT_UNID_PATY
           WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D
            AND SRCE_SYST_C = 'SAP' 
            AND PATY_ACCT_REL_C = 'ACTO') UIP 
ON UIP.SRCE_SYST_PATY_I = FCLY.SRCE_SYST_PATY_I 
WHERE (
       (UIP.EFFT_D BETWEEN FCLY.EFFT_D AND FCLY.EXPY_D) 
        OR (FCLY.EFFT_D BETWEEN UIP.EFFT_D AND UIP.EXPY_D)
      )
 
  
) AX 

ON AX.ASSC_ACCT_I = AP.ACCT_I

WHERE (
       (AX.EFFT_D BETWEEN AP.EFFT_D AND AP.EXPY_D) 
        OR (AP.EFFT_D BETWEEN AX.EFFT_D AND AX.EXPY_D)
      )
 

GROUP BY 1,2,3,4,5,6,7,8,9;

COLLECT STATS {{ bteq_var("DDSTG") }}.DERV_ACCT_PATY_CURR;


-- $LastChangedBy: zakhe $
-- $LastChangedDate: 2013-11-06 12:00:30 +1100 (Wed, 06 Nov 2013) $
-- $LastChangedRevision: 12976 $

