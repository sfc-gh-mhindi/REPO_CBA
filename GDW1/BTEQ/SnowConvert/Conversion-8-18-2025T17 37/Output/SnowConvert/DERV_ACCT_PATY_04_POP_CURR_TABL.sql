!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.RUN FILE=%%BTEQ_LOGON_SCRIPT%%

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.IF ERRORLEVEL <> 0 THEN .GOTO EXITERR

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!

.SET QUIET OFF

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.SET ECHOREQ ON

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.SET FORMAT OFF

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.SET WIDTH 120

--------------------------------------------------------------------------------
---- Object Name             :  DERV_ACCT_PATY_04_POP_CURR_TABL.sql
---- Object Type             :  BTEQ

----                           
---- Description             :  Populate staging table with the rows from all sources
----                            with the rows effective on extract date
----                          
--------------------------------------------------------------------------------
---- Modification History
---- Date               Author           Version     Version Description
---- 04/06/2013         Helen Zak        1.0         Initial Version
---- 31/07/2014         Megan Disch      1.1         Flip MID, MTX and LMS ACCT_UNID_PATY ACCT_I and ASSC_ACCT_I
---- 07/08/2013         Helen Zak        1.2         Collect stats after the last insert of MOS accounts
----                                                 Include error processing where it's missing
---- 21/08/2013         Helen Zak        1.3         C0726912 - post-implementation fix
----                                                 Get all effective rows from all sources
---- 06/11/2013         Helen Zak       1.4         C0800027  R32 Nexus
----                                                                          Include additional join condition for LMS, MID and MTX accounts
---- 21/07/2016   A&IPlatformRRTSquad@cba.com.au 
----				       1.5         C2151903  WSS :Change Internal deal paty_i from External Deal to Internal Deal. 
----            


--------------------------------------------------------------------------------



---- insert into DERV_PATY_ALL_CURR from all available sources
---- but only rows that were effective on the extract date.
---- If a row expired ON or AFTER extract date, push its expiry date to '9999-12-31'
---- so that an account looks like it did on the extract date.
---- The only rows that expired on extract date that are included are those with
---- effective date being the same as expiry date

----1. everything from ACCT_PATY that was effective
---- on that date. Remove duplicates from ACCT_PATY first
--!!!RESOLVE EWI!!! /*** SSC-EWI-0013 - EXCEPTION THROWN WHILE CONVERTING ITEM: Mobilize.T12Data.Sql.Ast.TdDelete. LINE: 48 OF FILE: /Users/mhindi/Library/CloudStorage/GoogleDrive-mazen.hindi@snowflake.com/My Drive/Work/Code Repos/CBA/REPO_CBA/GDW1/BTEQ/DERV_ACCT_PATY_04_POP_CURR_TABL.sql ***/!!!

--DELETE
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '48' COLUMN '7' OF THE SOURCE CODE STARTING AT 'FROM'. EXPECTED 'FROM' GRAMMAR. LAST MATCHING TOKEN WAS 'FROM' ON LINE '48' COLUMN '7'. FAILED TOKEN WAS '%' ON LINE '48' COLUMN '12'. **
--       FROM %%DDSTG%%.ACCT_PATY_DEDUP
                                     ;

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.IF ERRORCODE   <> 0 THEN .GOTO EXITERR

-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '51' COLUMN '1' OF THE SOURCE CODE STARTING AT 'COLLECT'. EXPECTED 'STATEMENT' GRAMMAR. LAST MATCHING TOKEN WAS 'COLLECT' ON LINE '51' COLUMN '1'. **
--COLLECT STATS %%DDSTG%%.ACCT_PATY_DEDUP
                                       ;

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.IF ERRORCODE   <> 0 THEN .GOTO EXITERR
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '54' COLUMN '2' OF THE SOURCE CODE STARTING AT 'IMPORT'. EXPECTED 'STATEMENT' GRAMMAR. LAST MATCHING TOKEN WAS 'IMPORT' ON LINE '54' COLUMN '2'. **
--                                       IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_extr_date.txt
USING
( EXTR_D VARCHAR(10) )
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '57' COLUMN '1' OF THE SOURCE CODE STARTING AT 'INSERT'. EXPECTED 'Insert' GRAMMAR. LAST MATCHING TOKEN WAS 'INTO' ON LINE '57' COLUMN '8'. **
--INSERT INTO %%DDSTG%%.ACCT_PATY_DEDUP
--SEL AP.ACCT_I
--,PATY_I
--,AP.ACCT_I AS ASSC_ACCT_I
--,PATY_ACCT_REL_C
--,'N'
--,SRCE_SYST_C
--,EFFT_D
--,CASE
--        WHEN EFFT_D = EXPY_D THEN EXPY_D
--        WHEN EXPY_D >= :EXTR_D THEN DATE'9999-12-31'
--        ELSE EXPY_D
--  END AS EXPY_D
--,AP.ROW_SECU_ACCS_C

--FROM %%VTECH%%.ACCT_PATY AP

--WHERE :EXTR_D BETWEEN AP.EFFT_D AND AP.EXPY_D
--QUALIFY ROW_NUMBER() OVER (PARTITION BY ACCT_I, PATY_I, PATY_ACCT_REL_C ORDER BY EFFT_D) = 1

--;

-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '79' COLUMN '1' OF THE SOURCE CODE STARTING AT '.'. EXPECTED 'STATEMENT' GRAMMAR. LAST MATCHING TOKEN WAS ';' ON LINE '77' COLUMN '1'. FAILED TOKEN WAS '.' ON LINE '79' COLUMN '1'. **
--.
 ;
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '79' COLUMN '2' OF THE SOURCE CODE STARTING AT 'IF'. EXPECTED 'STATEMENT' GRAMMAR. LAST MATCHING TOKEN WAS 'IMPORT' ON LINE '54' COLUMN '2'. FAILED TOKEN WAS 'IF' ON LINE '79' COLUMN '2'. **
--  IF ERRORCODE   <> 0 THEN .GOTO EXITERR


--COLLECT STATS %%DDSTG%%.ACCT_PATY_DEDUP
                                       ;

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.IF ERRORCODE   <> 0 THEN .GOTO EXITERR

----Now insert into DERV_ACCT_PATY_CURR rows from ACCT_PATY without duplicates
---- and use this table for other extracts as well
--!!!RESOLVE EWI!!! /*** SSC-EWI-0013 - EXCEPTION THROWN WHILE CONVERTING ITEM: Mobilize.T12Data.Sql.Ast.TdDelete. LINE: 88 OF FILE: /Users/mhindi/Library/CloudStorage/GoogleDrive-mazen.hindi@snowflake.com/My Drive/Work/Code Repos/CBA/REPO_CBA/GDW1/BTEQ/DERV_ACCT_PATY_04_POP_CURR_TABL.sql ***/!!!

--DELETE
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '88' COLUMN '7' OF THE SOURCE CODE STARTING AT 'FROM'. EXPECTED 'FROM' GRAMMAR. LAST MATCHING TOKEN WAS 'FROM' ON LINE '88' COLUMN '7'. FAILED TOKEN WAS '%' ON LINE '88' COLUMN '12'. **
--       FROM %%DDSTG%%.DERV_ACCT_PATY_CURR
                                         ;

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.IF ERRORCODE   <> 0 THEN .GOTO EXITERR

-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '91' COLUMN '1' OF THE SOURCE CODE STARTING AT 'COLLECT'. EXPECTED 'STATEMENT' GRAMMAR. LAST MATCHING TOKEN WAS 'COLLECT' ON LINE '91' COLUMN '1'. **
--COLLECT STATS %%DDSTG%%.DERV_ACCT_PATY_CURR
                                           ;

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.IF ERRORCODE   <> 0 THEN .GOTO EXITERR
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '94' COLUMN '2' OF THE SOURCE CODE STARTING AT 'IMPORT'. EXPECTED 'STATEMENT' GRAMMAR. LAST MATCHING TOKEN WAS 'IMPORT' ON LINE '94' COLUMN '2'. **
--                                       IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_extr_date.txt
USING
( EXTR_D VARCHAR(10) )
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '97' COLUMN '1' OF THE SOURCE CODE STARTING AT 'INSERT'. EXPECTED 'Insert' GRAMMAR. LAST MATCHING TOKEN WAS 'INTO' ON LINE '97' COLUMN '8'. **
--INSERT INTO %%DDSTG%%.DERV_ACCT_PATY_CURR
--SEL ACCT_I
--,PATY_I
--,ASSC_ACCT_I
--,PATY_ACCT_REL_C
--,PRFR_PATY_F
--,SRCE_SYST_C
--,EFFT_D
--,EXPY_D
--,ROW_SECU_ACCS_C

--FROM %%DDSTG%%.ACCT_PATY_DEDUP

--;

-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '112' COLUMN '1' OF THE SOURCE CODE STARTING AT '.'. EXPECTED 'STATEMENT' GRAMMAR. LAST MATCHING TOKEN WAS ';' ON LINE '110' COLUMN '1'. FAILED TOKEN WAS '.' ON LINE '112' COLUMN '1'. **
--.
 ;
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '112' COLUMN '2' OF THE SOURCE CODE STARTING AT 'IF'. EXPECTED 'STATEMENT' GRAMMAR. LAST MATCHING TOKEN WAS 'IMPORT' ON LINE '94' COLUMN '2'. FAILED TOKEN WAS 'IF' ON LINE '112' COLUMN '2'. **
--  IF ERRORCODE   <> 0 THEN .GOTO EXITERR



--COLLECT STATS %%DDSTG%%.DERV_ACCT_PATY_CURR
                                           ;

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.IF ERRORCODE   <> 0 THEN .GOTO EXITERR
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '129' COLUMN '2' OF THE SOURCE CODE STARTING AT 'IMPORT'. EXPECTED 'STATEMENT' GRAMMAR. LAST MATCHING TOKEN WAS 'IMPORT' ON LINE '129' COLUMN '2'. **
--                                       IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_extr_date.txt
USING
( EXTR_D VARCHAR(10) )
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '132' COLUMN '2' OF THE SOURCE CODE STARTING AT 'INSERT'. EXPECTED 'Insert' GRAMMAR. LAST MATCHING TOKEN WAS 'INTO' ON LINE '132' COLUMN '9'. **
-- INSERT INTO %%DDSTG%%.DERV_ACCT_PATY_CURR
--SELECT AX.BPS_ACCT_I AS ACCT_I
--             ,AP.PATY_I
--            ,CBS_ACCT_I AS ASSC_ACCT_I
--            ,AP.PATY_ACCT_REL_C
--            ,'N' AS PRFR_PATY_F
--            ,AP.SRCE_SYST_C
--          ,(CASE WHEN AP.EFFT_D > AX.EFFT_D THEN AP.EFFT_D ELSE AX.EFFT_D END) AS EFFT_D
--          ,(CASE WHEN AP.EXPY_D < AX.EXPY_D THEN AP.EXPY_D ELSE AX.EXPY_D END) AS EXPY_D
--          ,AP.ROW_SECU_ACCS_C

-- FROM (
--                 SELECT ACCT_I
--                               ,PATY_I
--                               ,PATY_ACCT_REL_C
--                               ,SRCE_SYST_C
--                               ,EFFT_D
--                               ,CASE
--                                       WHEN EFFT_D = EXPY_D THEN EXPY_D
--                                       WHEN EXPY_D >= :EXTR_D THEN DATE'9999-12-31'
--                                      ELSE EXPY_D
--                                END AS EXPY_D
--                             ,ROW_SECU_ACCS_C
--                 FROM   %%DDSTG%%.ACCT_PATY_DEDUP
--              WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D
--          ) AP
--  JOIN (
--              SELECT CBS_ACCT_I
--                           ,BPS_ACCT_I
--                           ,EFFT_D
--                           ,CASE
--                                       WHEN EFFT_D = EXPY_D THEN EXPY_D
--                                       WHEN EXPY_D >= :EXTR_D THEN DATE'9999-12-31'
--                                      ELSE EXPY_D
--                            END AS EXPY_D
--                FROM   %%VTECH%%.ACCT_XREF_BPS_CBS
--              WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D
--          )   AX
--ON AP.ACCT_I = AX.CBS_ACCT_I
-- WHERE (
--                     (AP.EFFT_D BETWEEN AX.EFFT_D AND AX.EXPY_D)
--                OR (AX.EFFT_D BETWEEN AP.EFFT_D AND AP.EXPY_D)
--             )


--GROUP BY 1,2,3,4,5,6,7,8,9

--UNION ALL

--SELECT
--  AR.OBJC_ACCT_I AS ACCT_I
--, BPS.PATY_I
--, BPS.CBS_ACCT_I AS ASSC_ACCT_I
--, BPS.PATY_ACCT_REL_C
--,'N' AS PRFR_PATY_F
--, BPS.SRCE_SYST_C
--, (CASE WHEN AR.EFFT_D >  BPS.EFFT_D  THEN AR.EFFT_D  ELSE BPS.EFFT_D END) AS EFFT_D
--, (CASE WHEN AR.EXPY_D <  BPS.EXPY_D THEN AR.EXPY_D ELSE BPS.EXPY_D END) AS EXPY_D
-- ,BPS.ROW_SECU_ACCS_C

--FROM  (SEL SUBJ_ACCT_I
--                      ,OBJC_ACCT_I
--                      ,EFFT_D
--                      ,CASE
--                                WHEN EFFT_D = EXPY_D THEN EXPY_D
--                                WHEN EXPY_D >= :EXTR_D THEN DATE'9999-12-31'
--                                 ELSE EXPY_D
--                        END AS EXPY_D
--             FROM   %%VTECH%%.ACCT_REL
--           WHERE REL_C = 'FLBLL'
--                AND :EXTR_D BETWEEN EFFT_D AND EXPY_D ) AR
--JOIN

--(
--SELECT AX.BPS_ACCT_I
--            ,CBS_ACCT_I
--            ,AP.PATY_I
--            ,AP.PATY_ACCT_REL_C
--            ,AP.SRCE_SYST_C
--          ,(CASE WHEN AP.EFFT_D > AX.EFFT_D THEN AP.EFFT_D ELSE AX.EFFT_D END) AS EFFT_D
--          ,(CASE WHEN AP.EXPY_D < AX.EXPY_D THEN AP.EXPY_D ELSE AX.EXPY_D END) AS EXPY_D
--           ,AP.ROW_SECU_ACCS_C
-- FROM (
--                SEL ACCT_I
--                     ,PATY_I
--                     ,PATY_ACCT_REL_C
--                     ,SRCE_SYST_C
--                     ,EFFT_D
--                     ,CASE
--                                WHEN EFFT_D = EXPY_D THEN EXPY_D
--                                WHEN EXPY_D >= :EXTR_D THEN DATE'9999-12-31'
--                                 ELSE EXPY_D
--                        END AS EXPY_D
--                     ,ROW_SECU_ACCS_C
--           FROM    %%DDSTG%%.ACCT_PATY_DEDUP
--             WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D)  AP

--JOIN  (
--              SELECT CBS_ACCT_I
--                           ,BPS_ACCT_I
--                           ,EFFT_D
--                           ,CASE
--                                      WHEN EFFT_D = EXPY_D THEN EXPY_D
--                                       WHEN EXPY_D >= :EXTR_D THEN DATE'9999-12-31'
--                                      ELSE EXPY_D
--                            END AS EXPY_D
--                FROM   %%VTECH%%.ACCT_XREF_BPS_CBS
--              WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D
--          )   AX
--ON AP.ACCT_I = AX.CBS_ACCT_I
--WHERE  (
--                     (AP.EFFT_D BETWEEN AX.EFFT_D AND AX.EXPY_D)
--                OR (AX.EFFT_D BETWEEN AP.EFFT_D AND AP.EXPY_D)
--                   )



--        GROUP BY 1, 2, 3, 4, 5,6,7,8

--) BPS
--ON AR.SUBJ_ACCT_I = BPS.BPS_ACCT_I

--WHERE (
--                (AR.EFFT_D BETWEEN BPS.EFFT_D AND BPS.EXPY_D) OR
--                (BPS.EFFT_D BETWEEN AR.EFFT_D AND AR.EXPY_D)
--              )
--GROUP BY 1,2,3,4,5,6,7,8,9

--;
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '261' COLUMN '1' OF THE SOURCE CODE STARTING AT '.'. EXPECTED 'STATEMENT' GRAMMAR. LAST MATCHING TOKEN WAS ';' ON LINE '260' COLUMN '1'. FAILED TOKEN WAS '.' ON LINE '261' COLUMN '1'. **
--.
 ;
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '261' COLUMN '2' OF THE SOURCE CODE STARTING AT 'IF'. EXPECTED 'STATEMENT' GRAMMAR. LAST MATCHING TOKEN WAS 'IMPORT' ON LINE '129' COLUMN '2'. FAILED TOKEN WAS 'IF' ON LINE '261' COLUMN '2'. **
--  IF ERRORCODE   <> 0 THEN .GOTO EXITERR



--COLLECT STATS %%DDSTG%%.DERV_ACCT_PATY_CURR
                                           ;

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.IF ERRORCODE   <> 0 THEN .GOTO EXITERR
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '275' COLUMN '2' OF THE SOURCE CODE STARTING AT 'IMPORT'. EXPECTED 'STATEMENT' GRAMMAR. LAST MATCHING TOKEN WAS 'IMPORT' ON LINE '275' COLUMN '2'. **
--                                       IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_extr_date.txt
USING
( EXTR_D VARCHAR(10) )
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '278' COLUMN '1' OF THE SOURCE CODE STARTING AT 'INSERT'. EXPECTED 'Insert' GRAMMAR. LAST MATCHING TOKEN WAS 'INTO' ON LINE '278' COLUMN '8'. **
--INSERT INTO %%DDSTG%%.DERV_ACCT_PATY_CURR
-- SEL CLS.ACCT_I
--      ,AP.PATY_I
--     ,CLS.GDW_ACCT_I AS ASSC_ACCT_I
--     ,AP.PATY_ACCT_REL_C
--     ,'N' AS PRFR_PATY_F
--     ,AP.SRCE_SYST_C
--   , (CASE WHEN AP.EFFT_D > CLS.EFFT_D THEN AP.EFFT_D ELSE CLS.EFFT_D END) AS EFFT_D
--   , (CASE WHEN AP.EXPY_D < CLS.EXPY_D THEN AP.EXPY_D ELSE CLS.EXPY_D END) AS EXPY_D
--   ,AP.ROW_SECU_ACCS_C
--FROM  (
--                SEL ACCT_I
--                     ,PATY_I
--                     ,PATY_ACCT_REL_C
--                     ,SRCE_SYST_C
--                     ,EFFT_D
--                     ,CASE
--                                WHEN EFFT_D = EXPY_D THEN EXPY_D
--                                WHEN EXPY_D >= :EXTR_D THEN DATE'9999-12-31'
--                                 ELSE EXPY_D
--                        END AS EXPY_D
--                     ,ROW_SECU_ACCS_C
--           FROM    %%DDSTG%%.ACCT_PATY_DEDUP
--             WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D)  AP

--JOIN
--    (SEL CF.ACCT_I
--    , 'CLSCO'||TRIM(CUP.CRIS_DEBT_I) AS GDW_ACCT_I
--    ,(CASE WHEN CF.EFFT_D > CUP.EFFT_D THEN CF.EFFT_D ELSE CUP.EFFT_D END) AS EFFT_D
--    ,(CASE WHEN CF.EXPY_D < CUP.EXPY_D THEN CF.EXPY_D ELSE CUP.EXPY_D END) AS EXPY_D
--     FROM  (SEL ACCT_I
--                          ,SRCE_SYST_PATY_I
--                         ,EFFT_D
--                         ,CASE
--                                WHEN EFFT_D = EXPY_D THEN EXPY_D
--                                WHEN EXPY_D >= :EXTR_D THEN DATE'9999-12-31'
--                                 ELSE EXPY_D
--                        END AS EXPY_D
--             FROM %%VTECH%%.CLS_FCLY
--             WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D)  CF

--     JOIN  (
--                   SEL SRCE_SYST_PATY_I
--                         ,CRIS_DEBT_I
--                         ,EFFT_D
--                         ,CASE
--                                WHEN EFFT_D = EXPY_D THEN EXPY_D
--                                WHEN EXPY_D >= :EXTR_D THEN DATE'9999-12-31'
--                                 ELSE EXPY_D
--                        END AS EXPY_D
--                  FROM %%VTECH%%.CLS_UNID_PATY
--                  WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D) CUP
--     ON CUP.SRCE_SYST_PATY_I  = CF.SRCE_SYST_PATY_I
--     WHERE (
--            (CUP.EFFT_D BETWEEN CF.EFFT_D AND CF.EXPY_D)
--        OR (CF.EFFT_D BETWEEN CUP.EFFT_D AND CUP.EXPY_D)
--       )

--        GROUP BY 1, 2, 3, 4
--) AS CLS
--ON CLS.GDW_ACCT_I = AP.ACCT_I
--WHERE  (
--        (AP.EFFT_D BETWEEN CLS.EFFT_D AND CLS.EXPY_D) OR
--         (CLS.EFFT_D BETWEEN AP.EFFT_D AND AP.EXPY_D)
--      )


--GROUP BY 1,2,3,4,5,6,7,8,9
--;
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '347' COLUMN '1' OF THE SOURCE CODE STARTING AT '.'. EXPECTED 'STATEMENT' GRAMMAR. LAST MATCHING TOKEN WAS ';' ON LINE '346' COLUMN '1'. FAILED TOKEN WAS '.' ON LINE '347' COLUMN '1'. **
--.
 ;
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '347' COLUMN '2' OF THE SOURCE CODE STARTING AT 'IF'. EXPECTED 'STATEMENT' GRAMMAR. LAST MATCHING TOKEN WAS 'IMPORT' ON LINE '275' COLUMN '2'. FAILED TOKEN WAS 'IF' ON LINE '347' COLUMN '2'. **
--  IF ERRORCODE   <> 0 THEN .GOTO EXITERR



--COLLECT STATS %%DDSTG%%.DERV_ACCT_PATY_CURR
                                           ;

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.IF ERRORCODE   <> 0 THEN .GOTO EXITERR
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '362' COLUMN '2' OF THE SOURCE CODE STARTING AT 'IMPORT'. EXPECTED 'STATEMENT' GRAMMAR. LAST MATCHING TOKEN WAS 'IMPORT' ON LINE '362' COLUMN '2'. **
--                                       IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_extr_date.txt
USING
( EXTR_D VARCHAR(10) )
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '365' COLUMN '1' OF THE SOURCE CODE STARTING AT 'INSERT'. EXPECTED 'Insert' GRAMMAR. LAST MATCHING TOKEN WAS 'INTO' ON LINE '365' COLUMN '8'. **
--INSERT INTO %%DDSTG%%.DERV_ACCT_PATY_CURR
--SELECT DAR.MERC_ACCT_I AS ACCT_I
--           , AP.PATY_I
--            ,DAR.MAS_MERC_ACCT_I
--           , AP.PATY_ACCT_REL_C
--           ,'N' AS PRFR_PATY_I
--           , AP.SRCE_SYST_C
--          , (CASE WHEN AP.EFFT_D >  DAR.EFFT_D  THEN AP.EFFT_D  ELSE DAR.EFFT_D END) AS EFFT_D
--          , (CASE WHEN AP.EXPY_D <  DAR.EXPY_D THEN AP.EXPY_D ELSE DAR.EXPY_D END) AS EXPY_D
--           ,AP.ROW_SECU_ACCS_C
--FROM   (
--                SEL ACCT_I
--                     ,PATY_I
--                     ,PATY_ACCT_REL_C
--                     ,SRCE_SYST_C
--                     ,EFFT_D
--                     ,CASE
--                                WHEN EFFT_D = EXPY_D THEN EXPY_D
--                                WHEN EXPY_D >= :EXTR_D THEN DATE'9999-12-31'
--                                 ELSE EXPY_D
--                        END AS EXPY_D
--                     ,ROW_SECU_ACCS_C
--           FROM    %%DDSTG%%.ACCT_PATY_DEDUP
--             WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D)  AP


--JOIN (

--SELECT DA.MERC_ACCT_I
--             ,AX.MAS_MERC_ACCT_I
--              ,(CASE WHEN DA.EFFT_D > AX.EFFT_D THEN DA.EFFT_D ELSE AX.EFFT_D END) AS EFFT_D
--               ,(CASE WHEN DA.EXPY_D < AX.EXPY_D THEN DA.EXPY_D ELSE AX.EXPY_D END) AS EXPY_D
--FROM
--(SEL  MERC_ACCT_I
--         ,EFFT_D
--         ,CASE
--                 WHEN EFFT_D = EXPY_D THEN EXPY_D
--                 WHEN EXPY_D >= :EXTR_D THEN DATE'9999-12-31'
--                    ELSE EXPY_D
--       END AS EXPY_D
--   FROM %%VTECH%%.DAR_ACCT
--   WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D) DA
--JOIN (
--SELECT MAS_MERC_ACCT_I
--             ,DAR_ACCT_I
--             ,EFFT_D
--              ,CASE
--                 WHEN EFFT_D = EXPY_D THEN EXPY_D
--                 WHEN EXPY_D >= :EXTR_D THEN DATE'9999-12-31'
--                    ELSE EXPY_D
--              END AS EXPY_D
--     FROM        %%VTECH%%.ACCT_XREF_MAS_DAR
--     WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D)  AX
--  ON AX.DAR_ACCT_I = DA.MERC_ACCT_I
--WHERE (
--               (AX.EFFT_D BETWEEN DA.EFFT_D AND DA.EXPY_D)
--         OR (DA.EFFT_D BETWEEN AX.EFFT_D AND AX.EXPY_D)
--              )

--GROUP BY 1, 2,3,4

--) AS DAR
--ON DAR.MAS_MERC_ACCT_I = AP.ACCT_I

--WHERE (
--               (AP.EFFT_D BETWEEN DAR.EFFT_D AND DAR.EXPY_D)
--         OR (DAR.EFFT_D BETWEEN AP.EFFT_D AND AP.EXPY_D)
--             )


--             ;

-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '437' COLUMN '1' OF THE SOURCE CODE STARTING AT '.'. EXPECTED 'STATEMENT' GRAMMAR. LAST MATCHING TOKEN WAS ';' ON LINE '435' COLUMN '14'. FAILED TOKEN WAS '.' ON LINE '437' COLUMN '1'. **
--.
 ;
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '437' COLUMN '2' OF THE SOURCE CODE STARTING AT 'IF'. EXPECTED 'STATEMENT' GRAMMAR. LAST MATCHING TOKEN WAS 'IMPORT' ON LINE '362' COLUMN '2'. FAILED TOKEN WAS 'IF' ON LINE '437' COLUMN '2'. **
--  IF ERRORCODE   <> 0 THEN .GOTO EXITERR

--COLLECT STATS %%DDSTG%%.DERV_ACCT_PATY_CURR
                                           ;

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.IF ERRORCODE   <> 0 THEN .GOTO EXITERR
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '444' COLUMN '2' OF THE SOURCE CODE STARTING AT 'IMPORT'. EXPECTED 'STATEMENT' GRAMMAR. LAST MATCHING TOKEN WAS 'IMPORT' ON LINE '444' COLUMN '2'. **
--                                       IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_extr_date.txt
USING
( EXTR_D VARCHAR(10) )
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '447' COLUMN '1' OF THE SOURCE CODE STARTING AT 'INSERT'. EXPECTED 'Insert' GRAMMAR. LAST MATCHING TOKEN WAS 'INTO' ON LINE '447' COLUMN '8'. **
--INSERT INTO %%DDSTG%%.DERV_ACCT_PATY_CURR
--SELECT AR.OBJC_ACCT_I AS ACCT_I
--     , AP.PATY_I
--     ,AR.SUBJ_ACCT_I AS ASSC_ACCT_I
--    , AP.PATY_ACCT_REL_C
--    ,'N' AS PRFR_PATY_F
--    , AP.SRCE_SYST_C
--, (CASE WHEN AR.EFFT_D >  AP.EFFT_D  THEN AR.EFFT_D  ELSE AP.EFFT_D END) AS EFFT_D
--, (CASE WHEN AR.EXPY_D <  AP.EXPY_D THEN AR.EXPY_D ELSE AP.EXPY_D END) AS EXPY_D
-- ,AP.ROW_SECU_ACCS_C
--FROM  (
--                SEL ACCT_I
--                     ,PATY_I
--                     ,PATY_ACCT_REL_C
--                     ,SRCE_SYST_C
--                     ,EFFT_D
--                     ,CASE
--                               WHEN EFFT_D = EXPY_D THEN EXPY_D
--                                WHEN EXPY_D >= :EXTR_D THEN DATE'9999-12-31'
--                                 ELSE EXPY_D
--                        END AS EXPY_D
--                     ,ROW_SECU_ACCS_C
--           FROM    %%DDSTG%%.ACCT_PATY_DEDUP
--             WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D)  AP

--JOIN (
--            SELECT AR1.SUBJ_ACCT_I
--                          ,AR1.OBJC_ACCT_I
--                          ,AR1.EFFT_D
--                          ,CASE
--                                WHEN AR1.EFFT_D = AR1.EXPY_D THEN AR1.EXPY_D
--                                WHEN AR1.EXPY_D >= :EXTR_D THEN DATE'9999-12-31'
--                                 ELSE AR1.EXPY_D
--                        END AS EXPY_D
--                     FROM %%VTECH%%.ACCT_REL AR1
--                        JOIN %%DDSTG%%.GRD_GNRC_MAP_DERV_PATY_REL GGM
--                        ON AR1.REL_C = GGM.REL_C
--                     AND GGM.ACCT_I_C = 'SUBJ'
--                     AND :EXTR_D BETWEEN AR1.EFFT_D AND AR1.EXPY_D ) AR
--  ON AR.SUBJ_ACCT_I = AP.ACCT_I

--WHERE ( (AR.EFFT_D BETWEEN AP.EFFT_D AND AP.EXPY_D)
--        OR  (AP.EFFT_D BETWEEN AR.EFFT_D AND AR.EXPY_D)
--    )



--GROUP BY 1,2,3,4,5,6,7,8,9
--;
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '496' COLUMN '1' OF THE SOURCE CODE STARTING AT '.'. EXPECTED 'STATEMENT' GRAMMAR. LAST MATCHING TOKEN WAS ';' ON LINE '495' COLUMN '1'. FAILED TOKEN WAS '.' ON LINE '496' COLUMN '1'. **
--.
 ;
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '496' COLUMN '2' OF THE SOURCE CODE STARTING AT 'IF'. EXPECTED 'STATEMENT' GRAMMAR. LAST MATCHING TOKEN WAS 'IMPORT' ON LINE '444' COLUMN '2'. FAILED TOKEN WAS 'IF' ON LINE '496' COLUMN '2'. **
--  IF ERRORCODE   <> 0 THEN .GOTO EXITERR



--COLLECT STATS %%DDSTG%%.DERV_ACCT_PATY_CURR
                                           ;

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.IF ERRORCODE   <> 0 THEN .GOTO EXITERR
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '505' COLUMN '2' OF THE SOURCE CODE STARTING AT 'IMPORT'. EXPECTED 'STATEMENT' GRAMMAR. LAST MATCHING TOKEN WAS 'IMPORT' ON LINE '505' COLUMN '2'. **
--                                       IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_extr_date.txt
USING
( EXTR_D VARCHAR(10) )
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '508' COLUMN '1' OF THE SOURCE CODE STARTING AT 'INSERT'. EXPECTED 'Insert' GRAMMAR. LAST MATCHING TOKEN WAS 'INTO' ON LINE '508' COLUMN '8'. **
--INSERT INTO %%DDSTG%%.DERV_ACCT_PATY_CURR
--SELECT AR.SUBJ_ACCT_I AS ACCT_I
--     , AP.PATY_I
--     ,AR.OBJC_ACCT_I AS ASSC_ACCT_I
--    , AP.PATY_ACCT_REL_C
--    ,'N' AS PRFR_PATY_F
--    , AP.SRCE_SYST_C
--, (CASE WHEN AR.EFFT_D >  AP.EFFT_D  THEN AR.EFFT_D  ELSE AP.EFFT_D END) AS EFFT_D
--, (CASE WHEN AR.EXPY_D <  AP.EXPY_D THEN AR.EXPY_D ELSE AP.EXPY_D END) AS EXPY_D
-- ,AP.ROW_SECU_ACCS_C
--FROM (
--                SEL ACCT_I
--                     ,PATY_I
--                     ,PATY_ACCT_REL_C
--                     ,SRCE_SYST_C
--                     ,EFFT_D
--                     ,CASE
--                               WHEN EFFT_D = EXPY_D THEN EXPY_D
--                                WHEN EXPY_D >= :EXTR_D THEN DATE'9999-12-31'
--                                 ELSE EXPY_D
--                        END AS EXPY_D
--                     ,ROW_SECU_ACCS_C
--           FROM    %%DDSTG%%.ACCT_PATY_DEDUP
--             WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D)  AP

--JOIN (
--            SELECT AR1.SUBJ_ACCT_I
--                          ,AR1.OBJC_ACCT_I
--                          ,AR1.EFFT_D
--                          ,CASE
--                                WHEN AR1.EFFT_D = AR1.EXPY_D THEN AR1.EXPY_D
--                                WHEN AR1.EXPY_D >= :EXTR_D THEN DATE'9999-12-31'
--                                 ELSE AR1.EXPY_D
--                        END AS EXPY_D
--                     FROM %%VTECH%%.ACCT_REL AR1
--                        JOIN %%DDSTG%%.GRD_GNRC_MAP_DERV_PATY_REL GGM
--                        ON AR1.REL_C = GGM.REL_C
--                     AND GGM.ACCT_I_C = 'OBJC'
--                     AND :EXTR_D BETWEEN AR1.EFFT_D AND AR1.EXPY_D ) AR
--  ON AR.OBJC_ACCT_I = AP.ACCT_I

--WHERE ( (AR.EFFT_D BETWEEN AP.EFFT_D AND AP.EXPY_D)
--        OR  (AP.EFFT_D BETWEEN AR.EFFT_D AND AR.EXPY_D)
--    )


--GROUP BY 1,2,3,4,5,6,7,8,9
--;
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '556' COLUMN '1' OF THE SOURCE CODE STARTING AT '.'. EXPECTED 'STATEMENT' GRAMMAR. LAST MATCHING TOKEN WAS ';' ON LINE '555' COLUMN '1'. FAILED TOKEN WAS '.' ON LINE '556' COLUMN '1'. **
--.
 ;
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '556' COLUMN '2' OF THE SOURCE CODE STARTING AT 'IF'. EXPECTED 'STATEMENT' GRAMMAR. LAST MATCHING TOKEN WAS 'IMPORT' ON LINE '505' COLUMN '2'. FAILED TOKEN WAS 'IF' ON LINE '556' COLUMN '2'. **
--  IF ERRORCODE   <> 0 THEN .GOTO EXITERR



--COLLECT STATS %%DDSTG%%.DERV_ACCT_PATY_CURR
                                           ;

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.IF ERRORCODE   <> 0 THEN .GOTO EXITERR

----7. Process WSS accounts
--!!!RESOLVE EWI!!! /*** SSC-EWI-0013 - EXCEPTION THROWN WHILE CONVERTING ITEM: Mobilize.T12Data.Sql.Ast.TdDelete. LINE: 565 OF FILE: /Users/mhindi/Library/CloudStorage/GoogleDrive-mazen.hindi@snowflake.com/My Drive/Work/Code Repos/CBA/REPO_CBA/GDW1/BTEQ/DERV_ACCT_PATY_04_POP_CURR_TABL.sql ***/!!!

--DEL
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '565' COLUMN '4' OF THE SOURCE CODE STARTING AT 'FROM'. EXPECTED 'FROM' GRAMMAR. LAST MATCHING TOKEN WAS 'FROM' ON LINE '565' COLUMN '4'. FAILED TOKEN WAS '%' ON LINE '565' COLUMN '9'. **
--    FROM %%DDSTG%%.ACCT_PATY_REL_WSS
                                    ;

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.IF ERRORCODE   <> 0 THEN .GOTO EXITERR

-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '568' COLUMN '1' OF THE SOURCE CODE STARTING AT 'COLLECT'. EXPECTED 'STATEMENT' GRAMMAR. LAST MATCHING TOKEN WAS 'COLLECT' ON LINE '568' COLUMN '1'. **
--COLLECT STATS %%DDSTG%%.ACCT_PATY_REL_WSS
                                         ;

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.IF ERRORCODE   <> 0 THEN .GOTO EXITERR
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '571' COLUMN '2' OF THE SOURCE CODE STARTING AT 'IMPORT'. EXPECTED 'STATEMENT' GRAMMAR. LAST MATCHING TOKEN WAS 'IMPORT' ON LINE '571' COLUMN '2'. **
--                                       IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_extr_date.txt
USING
( EXTR_D VARCHAR(10) )
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '574' COLUMN '1' OF THE SOURCE CODE STARTING AT 'INSERT'. EXPECTED 'Insert' GRAMMAR. LAST MATCHING TOKEN WAS 'INTO' ON LINE '574' COLUMN '8'. **
--INSERT INTO %%DDSTG%%.ACCT_PATY_REL_WSS
--SELECT AR.SUBJ_ACCT_I AS ACCT_I
--            , AP.PATY_I
--             ,AR.OBJC_ACCT_I AS ASSC_ACCT_I
--            , AP.PATY_ACCT_REL_C
--            ,'N' AS PRFR_PATY_F
--            , AP.SRCE_SYST_C
--            , (CASE WHEN AR.EFFT_D >  AP.EFFT_D  THEN AR.EFFT_D  ELSE AP.EFFT_D END) AS EFFT_D
--           , (CASE WHEN AR.EXPY_D <  AP.EXPY_D THEN AR.EXPY_D ELSE AP.EXPY_D END) AS EXPY_D
--           ,AP.ROW_SECU_ACCS_C

-- FROM (
--                SEL ACCT_I
--                     ,PATY_I
--                     ,PATY_ACCT_REL_C
--                     ,SRCE_SYST_C
--                     ,EFFT_D
--                     ,CASE
--                                WHEN EFFT_D = EXPY_D THEN EXPY_D
--                                WHEN EXPY_D >= :EXTR_D THEN DATE'9999-12-31'
--                                 ELSE EXPY_D
--                        END AS EXPY_D
--                     ,ROW_SECU_ACCS_C
--           FROM    %%DDSTG%%.ACCT_PATY_DEDUP
--             WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D)  AP
--   JOIN  (
--            SELECT AR1.SUBJ_ACCT_I
--                          ,AR1.OBJC_ACCT_I
--                          ,AR1.EFFT_D
--                          ,CASE
--                                WHEN AR1.EFFT_D = AR1.EXPY_D THEN AR1.EXPY_D
--                                WHEN AR1.EXPY_D >= :EXTR_D THEN DATE'9999-12-31'
--                                 ELSE AR1.EXPY_D
--                        END AS EXPY_D
--                     FROM %%VTECH%%.ACCT_REL AR1
--                      WHERE  AR1.REL_C = 'FCLYO'
--                     AND :EXTR_D BETWEEN AR1.EFFT_D AND AR1.EXPY_D ) AR
--     ON AR.OBJC_ACCT_I = AP.ACCT_I

--WHERE ( (AR.EFFT_D BETWEEN AP.EFFT_D AND AP.EXPY_D)
--        OR  (AP.EFFT_D BETWEEN AR.EFFT_D AND AR.EXPY_D)
--        )


--;

-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '620' COLUMN '1' OF THE SOURCE CODE STARTING AT '.'. EXPECTED 'STATEMENT' GRAMMAR. LAST MATCHING TOKEN WAS ';' ON LINE '618' COLUMN '1'. FAILED TOKEN WAS '.' ON LINE '620' COLUMN '1'. **
--.
 ;
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '620' COLUMN '2' OF THE SOURCE CODE STARTING AT 'IF'. EXPECTED 'STATEMENT' GRAMMAR. LAST MATCHING TOKEN WAS 'IMPORT' ON LINE '571' COLUMN '2'. FAILED TOKEN WAS 'IF' ON LINE '620' COLUMN '2'. **
--  IF ERRORCODE   <> 0 THEN .GOTO EXITERR


--COLLECT STATS %%DDSTG%%.ACCT_PATY_REL_WSS
                                         ;

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.IF ERRORCODE   <> 0 THEN .GOTO EXITERR


----Get the accounts that  also exist in ACCT_REL with DITPS relationship   
--!!!RESOLVE EWI!!! /*** SSC-EWI-0013 - EXCEPTION THROWN WHILE CONVERTING ITEM: Mobilize.T12Data.Sql.Ast.TdDelete. LINE: 629 OF FILE: /Users/mhindi/Library/CloudStorage/GoogleDrive-mazen.hindi@snowflake.com/My Drive/Work/Code Repos/CBA/REPO_CBA/GDW1/BTEQ/DERV_ACCT_PATY_04_POP_CURR_TABL.sql ***/!!!

--DELETE
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '629' COLUMN '7' OF THE SOURCE CODE STARTING AT 'FROM'. EXPECTED 'FROM' GRAMMAR. LAST MATCHING TOKEN WAS 'FROM' ON LINE '629' COLUMN '7'. FAILED TOKEN WAS '%' ON LINE '629' COLUMN '12'. **
--       FROM %%DDSTG%%.ACCT_REL_WSS_DITPS
                                        ;
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '631' COLUMN '3' OF THE SOURCE CODE STARTING AT 'IMPORT'. EXPECTED 'STATEMENT' GRAMMAR. LAST MATCHING TOKEN WAS 'IMPORT' ON LINE '631' COLUMN '3'. **
--                                         IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_extr_date.txt
USING
( EXTR_D VARCHAR(10) )
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '634' COLUMN '2' OF THE SOURCE CODE STARTING AT 'INSERT'. EXPECTED 'Insert' GRAMMAR. LAST MATCHING TOKEN WAS 'INTO' ON LINE '634' COLUMN '9'. **
-- INSERT INTO %%DDSTG%%.ACCT_REL_WSS_DITPS
-- SEL APA.ACCT_I
--  FROM %%DDSTG%%. ACCT_PATY_REL_WSS APA
--     JOIN   (
--            SELECT AR1.SUBJ_ACCT_I
--                          ,AR1.OBJC_ACCT_I
--                          ,AR1.EFFT_D
--                          ,CASE
--                                WHEN AR1.EFFT_D = AR1.EXPY_D THEN AR1.EXPY_D
--                                WHEN AR1.EXPY_D >= :EXTR_D THEN DATE'9999-12-31'
--                                 ELSE AR1.EXPY_D
--                        END AS EXPY_D
--                     FROM %%VTECH%%.ACCT_REL AR1
--                      WHERE  AR1.REL_C = 'DITPS'
--                     AND :EXTR_D BETWEEN AR1.EFFT_D AND AR1.EXPY_D ) AR
--        ON APA.ACCT_I = AR.OBJC_ACCT_I
--       AND (  (APA.EFFT_D BETWEEN AR.EFFT_D AND AR.EXPY_D)
--       OR  (AR.EFFT_D BETWEEN APA.EFFT_D AND APA.EXPY_D))

--        GROUP BY 1
                   ;

       !!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.IF ERRORCODE   <> 0 THEN .GOTO EXITERR


-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '657' COLUMN '1' OF THE SOURCE CODE STARTING AT 'COLLECT'. EXPECTED 'STATEMENT' GRAMMAR. LAST MATCHING TOKEN WAS 'COLLECT' ON LINE '657' COLUMN '1'. **
--COLLECT STATS %%DDSTG%%.ACCT_REL_WSS_DITPS
                                          ;

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.IF ERRORCODE   <> 0 THEN .GOTO EXITERR

--   --Delete those accounts that also exist in ACCT_REL with DITPS relationship. 
--!!!RESOLVE EWI!!! /*** SSC-EWI-0013 - EXCEPTION THROWN WHILE CONVERTING ITEM: Mobilize.T12Data.Sql.Ast.TdDelete. LINE: 662 OF FILE: /Users/mhindi/Library/CloudStorage/GoogleDrive-mazen.hindi@snowflake.com/My Drive/Work/Code Repos/CBA/REPO_CBA/GDW1/BTEQ/DERV_ACCT_PATY_04_POP_CURR_TABL.sql ***/!!!

--DEL APA
---- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '663' COLUMN '3' OF THE SOURCE CODE STARTING AT 'FROM'. EXPECTED 'FROM' GRAMMAR. LAST MATCHING TOKEN WAS 'FROM' ON LINE '663' COLUMN '3'. FAILED TOKEN WAS '%' ON LINE '663' COLUMN '8'. **
--  FROM %%DDSTG%%. ACCT_PATY_REL_WSS APA
--      , %%DDSTG%%.ACCT_REL_WSS_DITPS T1
-- WHERE
-- APA.ACCT_I = T1.ACCT_I
                       ;

 !!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!

.IF ERRORCODE   <> 0 THEN .GOTO EXITERR

-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '669' COLUMN '2' OF THE SOURCE CODE STARTING AT 'COLLECT'. EXPECTED 'STATEMENT' GRAMMAR. LAST MATCHING TOKEN WAS 'COLLECT' ON LINE '669' COLUMN '2'. **
-- COLLECT STATS %%DDSTG%%.ACCT_PATY_REL_WSS
                                          ;

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.IF ERRORCODE   <> 0 THEN .GOTO EXITERR
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '675' COLUMN '3' OF THE SOURCE CODE STARTING AT 'IMPORT'. EXPECTED 'STATEMENT' GRAMMAR. LAST MATCHING TOKEN WAS 'IMPORT' ON LINE '675' COLUMN '3'. **
--                                       IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_extr_date.txt
USING
( EXTR_D VARCHAR(10) )
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '678' COLUMN '3' OF THE SOURCE CODE STARTING AT 'INSERT'. EXPECTED 'Insert' GRAMMAR. LAST MATCHING TOKEN WAS 'INTO' ON LINE '678' COLUMN '10'. **
--  INSERT INTO %%DDSTG%%.ACCT_PATY_REL_WSS

-- SEL REL.ACCT_I
--      ,AP.PATY_I
--     ,REL.ASSC_ACCT_I
--     ,AP.PATY_ACCT_REL_C
--     ,'N' AS PRFR_PATY_F
--     ,AP.SRCE_SYST_C
--   , (CASE WHEN AP.EFFT_D > REL.EFFT_D THEN AP.EFFT_D ELSE REL.EFFT_D END) AS EFFT_D
--   , (CASE WHEN AP.EXPY_D < REL.EXPY_D THEN AP.EXPY_D ELSE REL.EXPY_D END) AS EXPY_D

--,AP.ROW_SECU_ACCS_C

--FROM  (
--                SEL ACCT_I
--                     ,PATY_I
--                     ,PATY_ACCT_REL_C
--                     ,SRCE_SYST_C
--                     ,EFFT_D
--                     ,CASE
--                                WHEN EFFT_D = EXPY_D THEN EXPY_D
--                                WHEN EXPY_D >= :EXTR_D THEN DATE'9999-12-31'
--                                 ELSE EXPY_D
--                        END AS EXPY_D
--                     ,ROW_SECU_ACCS_C
--           FROM    %%DDSTG%%.ACCT_PATY_DEDUP
--             WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D) AP
--JOIN
--    (SEL DITPS.OBJC_ACCT_I AS ACCT_I
--        ,FCLYO.OBJC_ACCT_I AS ASSC_ACCT_I
--    ,(CASE WHEN FCLYO.EFFT_D > DITPS.EFFT_D THEN FCLYO.EFFT_D ELSE DITPS.EFFT_D END) AS EFFT_D
--    ,(CASE WHEN FCLYO.EXPY_D < DITPS.EXPY_D THEN FCLYO.EXPY_D ELSE DITPS.EXPY_D END) AS EXPY_D
--     FROM  (
--            SELECT SUBJ_ACCT_I
--                          ,OBJC_ACCT_I
--                          ,EFFT_D
--                          ,CASE
--                                WHEN EFFT_D = EXPY_D THEN EXPY_D
--                                WHEN EXPY_D >= :EXTR_D THEN DATE'9999-12-31'
--                                 ELSE EXPY_D
--                        END AS EXPY_D
--                     FROM %%VTECH%%.ACCT_REL AR1
--                      WHERE  REL_C = 'FCLYO'
--                     AND :EXTR_D BETWEEN EFFT_D AND EXPY_D ) FCLYO

--     JOIN     (
--            SELECT SUBJ_ACCT_I
--                          ,OBJC_ACCT_I
--                          ,REL_C
--                          ,EFFT_D
--                          ,CASE
--                                WHEN EFFT_D = EXPY_D THEN EXPY_D
--                                WHEN EXPY_D >= :EXTR_D THEN DATE'9999-12-31'
--                                 ELSE EXPY_D
--                        END AS EXPY_D
--                     FROM %%VTECH%%.ACCT_REL AR1
--                      WHERE  REL_C = 'DITPS'
--                     AND :EXTR_D BETWEEN EFFT_D AND EXPY_D ) DITPS
--     ON FCLYO.SUBJ_ACCT_I = DITPS.OBJC_ACCT_I /* C2151903 - WSS DERV ACCT_PATY CHANGE */



--     JOIN %%DDSTG%%.ACCT_REL_WSS_DITPS AR3
--         ON DITPS.OBJC_ACCT_I = AR3.ACCT_I

--     WHERE (
--            (DITPS.EFFT_D BETWEEN FCLYO.EFFT_D AND FCLYO.EXPY_D)
--        OR (FCLYO.EFFT_D BETWEEN DITPS.EFFT_D AND DITPS.EXPY_D)
--       )
-- --this is to eliminate duplicate relationships that exist for some of DITPS 

--       QUALIFY ROW_NUMBER() OVER(PARTITION BY DITPS.OBJC_ACCT_I, DITPS.REL_C ORDER BY DITPS.SUBJ_ACCT_I DESC) = 1

--) AS REL
--ON REL.ASSC_ACCT_I = AP.ACCT_I

--WHERE (
--                (AP.EFFT_D BETWEEN REL.EFFT_D AND REL.EXPY_D) OR
--                (REL.EFFT_D BETWEEN AP.EFFT_D AND AP.EXPY_D)
--              )

--GROUP BY 1,2,3,4,5,6,7,8,9
--;
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '761' COLUMN '1' OF THE SOURCE CODE STARTING AT '.'. EXPECTED 'STATEMENT' GRAMMAR. LAST MATCHING TOKEN WAS ';' ON LINE '760' COLUMN '1'. FAILED TOKEN WAS '.' ON LINE '761' COLUMN '1'. **
--.
 ;
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '761' COLUMN '2' OF THE SOURCE CODE STARTING AT 'IF'. EXPECTED 'STATEMENT' GRAMMAR. LAST MATCHING TOKEN WAS 'IMPORT' ON LINE '675' COLUMN '3'. FAILED TOKEN WAS 'IF' ON LINE '761' COLUMN '2'. **
--  IF ERRORCODE   <> 0 THEN .GOTO EXITERR

--COLLECT STATS %%DDSTG%%.ACCT_PATY_REL_WSS
                                         ;

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.IF ERRORCODE   <> 0 THEN .GOTO EXITERR

-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '768' COLUMN '0' OF THE SOURCE CODE STARTING AT 'INSERT'. EXPECTED 'Insert' GRAMMAR. LAST MATCHING TOKEN WAS 'INTO' ON LINE '768' COLUMN '7'. **
----Insert WSS rows into the main table

--INSERT INTO %%DDSTG%%.DERV_ACCT_PATY_CURR
--SELECT ACCT_I
--,PATY_I
--,ASSC_ACCT_I
--,PATY_ACCT_REL_C
--,PRFR_PATY_F
--,SRCE_SYST_C
--,EFFT_D
--,EXPY_D
--,ROW_SECU_ACCS_C
--FROM %%DDSTG%%.ACCT_PATY_REL_WSS
                                ;

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.IF ERRORCODE   <> 0 THEN .GOTO EXITERR

-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '781' COLUMN '1' OF THE SOURCE CODE STARTING AT 'COLLECT'. EXPECTED 'STATEMENT' GRAMMAR. LAST MATCHING TOKEN WAS 'COLLECT' ON LINE '781' COLUMN '1'. **
--COLLECT STATS %%DDSTG%%.DERV_ACCT_PATY_CURR
                                           ;

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.IF ERRORCODE   <> 0 THEN .GOTO EXITERR

----8. THA accounts - convert using THA_ACCT
---- 1. Select only one trade account as per existing FPR logic 
----Select only the unique intersected (maximum of the EFFT_Ds and minimum of the EXPY_Ds) rows between the tables.  
----Retrieve the intersection of date ranges between the entries from the two tables:
----1. Find the intersection between ACCT_PATY and THA_ACCT.  
----2. Get the greater of the EFFT_D.
----3. Get the smaller of the EXPY_D.
--!!!RESOLVE EWI!!! /*** SSC-EWI-0013 - EXCEPTION THROWN WHILE CONVERTING ITEM: Mobilize.T12Data.Sql.Ast.TdDelete. LINE: 792 OF FILE: /Users/mhindi/Library/CloudStorage/GoogleDrive-mazen.hindi@snowflake.com/My Drive/Work/Code Repos/CBA/REPO_CBA/GDW1/BTEQ/DERV_ACCT_PATY_04_POP_CURR_TABL.sql ***/!!!

--DEL
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '792' COLUMN '4' OF THE SOURCE CODE STARTING AT 'FROM'. EXPECTED 'FROM' GRAMMAR. LAST MATCHING TOKEN WAS 'FROM' ON LINE '792' COLUMN '4'. FAILED TOKEN WAS '%' ON LINE '792' COLUMN '9'. **
--    FROM %%DDSTG%%.ACCT_PATY_REL_THA
                                    ;

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.IF ERRORCODE   <> 0 THEN .GOTO EXITERR

-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '795' COLUMN '1' OF THE SOURCE CODE STARTING AT 'COLLECT'. EXPECTED 'STATEMENT' GRAMMAR. LAST MATCHING TOKEN WAS 'COLLECT' ON LINE '795' COLUMN '1'. **
--COLLECT STATS %%DDSTG%%.ACCT_PATY_REL_THA
                                         ;

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.IF ERRORCODE   <> 0 THEN .GOTO EXITERR
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '798' COLUMN '2' OF THE SOURCE CODE STARTING AT 'IMPORT'. EXPECTED 'STATEMENT' GRAMMAR. LAST MATCHING TOKEN WAS 'IMPORT' ON LINE '798' COLUMN '2'. **
--                                       IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_extr_date.txt
USING
( EXTR_D VARCHAR(10) )
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '801' COLUMN '1' OF THE SOURCE CODE STARTING AT 'INSERT'. EXPECTED 'Insert' GRAMMAR. LAST MATCHING TOKEN WAS 'INTO' ON LINE '801' COLUMN '8'. **
--INSERT INTO %%DDSTG%%.ACCT_PATY_REL_THA

--  -- first get the max  

--     SEL THA_ACCT_I
--          ,TRAD_ACCT_I
--          ,EFFT_D
--          ,CASE
--                   WHEN EFFT_D = EXPY_D THEN EXPY_D
--                   WHEN EXPY_D >= :EXTR_D THEN DATE'9999-12-31'
--                    ELSE EXPY_D
--              END AS EXPY_D

--          FROM %%VTECH%%.THA_ACCT T1

--       WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D
--          QUALIFY ROW_NUMBER() OVER (PARTITION BY THA_ACCT_I, EFFT_D
-- ORDER BY TRAD_ACCT_I, CSL_CLNT_I DESC) = 1
                                           ;

 !!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
 .IF ERRORCODE   <> 0 THEN .GOTO EXITERR

-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '821' COLUMN '1' OF THE SOURCE CODE STARTING AT 'COLLECT'. EXPECTED 'STATEMENT' GRAMMAR. LAST MATCHING TOKEN WAS 'COLLECT' ON LINE '821' COLUMN '1'. **
--COLLECT STATS %%DDSTG%%.ACCT_PATY_REL_THA
                                         ;

 !!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.IF ERRORCODE   <> 0 THEN .GOTO EXITERR

-- !!!RESOLVE EWI!!! /*** SSC-EWI-0013 - EXCEPTION THROWN WHILE CONVERTING ITEM: Mobilize.T12Data.Sql.Ast.TdDelete. LINE: 824 OF FILE: /Users/mhindi/Library/CloudStorage/GoogleDrive-mazen.hindi@snowflake.com/My Drive/Work/Code Repos/CBA/REPO_CBA/GDW1/BTEQ/DERV_ACCT_PATY_04_POP_CURR_TABL.sql ***/!!!

--DEL
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '824' COLUMN '4' OF THE SOURCE CODE STARTING AT 'FROM'. EXPECTED 'FROM' GRAMMAR. LAST MATCHING TOKEN WAS 'FROM' ON LINE '824' COLUMN '4'. FAILED TOKEN WAS '%' ON LINE '824' COLUMN '9'. **
--    FROM %%DDSTG%%.ACCT_PATY_THA_NEW_RNGE
                                         ;

 !!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.IF ERRORCODE   <> 0 THEN .GOTO EXITERR

-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '827' COLUMN '1' OF THE SOURCE CODE STARTING AT 'COLLECT'. EXPECTED 'STATEMENT' GRAMMAR. LAST MATCHING TOKEN WAS 'COLLECT' ON LINE '827' COLUMN '1'. **
--COLLECT STATS %%DDSTG%%.ACCT_PATY_THA_NEW_RNGE
                                              ;

 !!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.IF ERRORCODE   <> 0 THEN .GOTO EXITERR

-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '830' COLUMN '0' OF THE SOURCE CODE STARTING AT 'INSERT'. EXPECTED 'Insert' GRAMMAR. LAST MATCHING TOKEN WAS 'INTO' ON LINE '830' COLUMN '7'. **
--INSERT INTO %%DDSTG%%.ACCT_PATY_THA_NEW_RNGE
--SEL THA_ACCT_I
--     ,TRAD_ACCT_I
--      ,EFFT_D
--     ,EXPY_D
--     ,CASE WHEN NEW_EXPY_D IS NULL THEN EXPY_D
--           WHEN NEW_EXPY_D <= EXPY_D AND NEW_EXPY_D > EFFT_D THEN NEW_EXPY_D - 1
--           ELSE EXPY_D
--       END AS NEW_EXPY_D
--       FROM

--(SELECT THA_ACCT_I
--             ,TRAD_ACCT_I
--             ,EFFT_D
--             ,EXPY_D
--             ,MIN(EFFT_D)
--             OVER(PARTITION BY THA_ACCT_I
--                         ORDER BY EFFT_D, EXPY_D
--                         ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING
--                        ) AS NEW_EXPY_D
--      FROM %%DDSTG%%.ACCT_PATY_REL_THA


--    ) T
       ;

      !!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!

.IF ERRORCODE   <> 0 THEN .GOTO EXITERR

-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '857' COLUMN '1' OF THE SOURCE CODE STARTING AT 'COLLECT'. EXPECTED 'STATEMENT' GRAMMAR. LAST MATCHING TOKEN WAS 'COLLECT' ON LINE '857' COLUMN '1'. **
--COLLECT STATS %%DDSTG%%.ACCT_PATY_THA_NEW_RNGE
                                              ;

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.IF ERRORCODE   <> 0 THEN .GOTO EXITERR

-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '860' COLUMN '3' OF THE SOURCE CODE STARTING AT 'INSERT'. EXPECTED 'Insert' GRAMMAR. LAST MATCHING TOKEN WAS 'INTO' ON LINE '860' COLUMN '10'. **
--   INSERT INTO %%DDSTG%%.ACCT_PATY_THA_NEW_RNGE
--     SEL T1.THA_ACCT_I
--     ,T1.TRAD_ACCT_I
--     ,T1.NEW_EXPY_D + 1 AS EFFT_D
--    ,T1.EXPY_D
--    ,T1.EXPY_D
--    FROM %%DDSTG%%.ACCT_PATY_THA_NEW_RNGE T1
--    LEFT JOIN %%DDSTG%%.ACCT_PATY_THA_NEW_RNGE T2

--    ON T1.THA_ACCT_I = T2.THA_ACCT_I

--    AND T1.NEW_EXPY_D + 1  = T2.EFFT_D
--    WHERE T1.NEW_EXPY_D < T1.EXPY_D AND T1.NEW_EXPY_D > T1.EFFT_D
--    AND T2.THA_ACCT_I IS NULL

--    GROUP BY 1,2,3,4,5
                      ;

    !!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!

.IF ERRORCODE   <> 0 THEN .GOTO EXITERR



-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '881' COLUMN '2' OF THE SOURCE CODE STARTING AT 'COLLECT'. EXPECTED 'STATEMENT' GRAMMAR. LAST MATCHING TOKEN WAS 'COLLECT' ON LINE '881' COLUMN '2'. **
-- COLLECT STATS %%DDSTG%%.ACCT_PATY_THA_NEW_RNGE
                                               ;

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
 .IF ERRORCODE   <> 0 THEN .GOTO EXITERR

--!!!RESOLVE EWI!!! /*** SSC-EWI-0013 - EXCEPTION THROWN WHILE CONVERTING ITEM: Mobilize.T12Data.Sql.Ast.TdDelete. LINE: 884 OF FILE: /Users/mhindi/Library/CloudStorage/GoogleDrive-mazen.hindi@snowflake.com/My Drive/Work/Code Repos/CBA/REPO_CBA/GDW1/BTEQ/DERV_ACCT_PATY_04_POP_CURR_TABL.sql ***/!!!

-- DEL
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '884' COLUMN '5' OF THE SOURCE CODE STARTING AT 'FROM'. EXPECTED 'FROM' GRAMMAR. LAST MATCHING TOKEN WAS 'FROM' ON LINE '884' COLUMN '5'. FAILED TOKEN WAS '%' ON LINE '884' COLUMN '10'. **
--     FROM %%DDSTG%%.ACCT_PATY_REL_THA
                                     ;

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
 .IF ERRORCODE   <> 0 THEN .GOTO EXITERR

-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '887' COLUMN '1' OF THE SOURCE CODE STARTING AT 'COLLECT'. EXPECTED 'STATEMENT' GRAMMAR. LAST MATCHING TOKEN WAS 'COLLECT' ON LINE '887' COLUMN '1'. **
--COLLECT STATS %%DDSTG%%.ACCT_PATY_REL_THA
                                         ;

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.IF ERRORCODE   <> 0 THEN .GOTO EXITERR

-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '890' COLUMN '0' OF THE SOURCE CODE STARTING AT 'INSERT'. EXPECTED 'Insert' GRAMMAR. LAST MATCHING TOKEN WAS 'INTO' ON LINE '890' COLUMN '7'. **
--INSERT INTO %%DDSTG%%.ACCT_PATY_REL_THA

--    SEL THA_ACCT_I
--          ,MAX(TRAD_ACCT_I)
--          ,EFFT_D
--          ,NEW_EXPY_D
--          FROM  %%DDSTG%%.ACCT_PATY_THA_NEW_RNGE

--          GROUP BY 1,3,4
                        ;

          !!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!

 .IF ERRORCODE   <> 0 THEN .GOTO EXITERR



-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '904' COLUMN '1' OF THE SOURCE CODE STARTING AT 'COLLECT'. EXPECTED 'STATEMENT' GRAMMAR. LAST MATCHING TOKEN WAS 'COLLECT' ON LINE '904' COLUMN '1'. **
--COLLECT STATS %%DDSTG%%.ACCT_PATY_REL_THA
                                         ;

 !!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.IF ERRORCODE   <> 0 THEN .GOTO EXITERR
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '907' COLUMN '2' OF THE SOURCE CODE STARTING AT 'IMPORT'. EXPECTED 'STATEMENT' GRAMMAR. LAST MATCHING TOKEN WAS 'IMPORT' ON LINE '907' COLUMN '2'. **
--                                       IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_extr_date.txt
USING
( EXTR_D VARCHAR(10) )
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '910' COLUMN '1' OF THE SOURCE CODE STARTING AT 'INSERT'. EXPECTED 'Insert' GRAMMAR. LAST MATCHING TOKEN WAS 'INTO' ON LINE '910' COLUMN '8'. **
--INSERT INTO %%DDSTG%%.DERV_ACCT_PATY_CURR
--SELECT ACCT_I
--            , PATY_I
--            , MAX(ASSC_ACCT_I)
--            , PATY_aCCT_REL_C
--            , PRFR_PATY_F
--            , SRCE_SYST_C
--            , EFFT_D
--            , EXPY_D
--             ,ROW_SECU_ACCS_C
--            FROM (

--SELECT TA.THA_ACCT_I AS ACCT_I
--, AP.PATY_I
--,TA.TRAD_ACCT_I AS ASSC_ACCT_I
--, AP.PATY_ACCT_REL_C
--,'N' AS PRFR_PATY_F
--, AP.SRCE_SYST_C
--, (CASE WHEN TA.EFFT_D >  AP.EFFT_D  THEN TA.EFFT_D  ELSE AP.EFFT_D END) AS EFFT_D
--, (CASE WHEN TA.EXPY_D <  AP.EXPY_D THEN TA.EXPY_D ELSE AP.EXPY_D END) AS EXPY_D
-- ,AP.ROW_SECU_ACCS_C
-- FROM   (
--                SEL ACCT_I
--                     ,PATY_I
--                     ,PATY_ACCT_REL_C
--                     ,SRCE_SYST_C
--                     ,EFFT_D
--                     ,CASE
--                                WHEN EFFT_D = EXPY_D THEN EXPY_D
--                                WHEN EXPY_D >= :EXTR_D THEN DATE'9999-12-31'
--                                 ELSE EXPY_D
--                        END AS EXPY_D
--                     ,ROW_SECU_ACCS_C
--           FROM    %%DDSTG%%.ACCT_PATY_DEDUP
--             WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D)  AP

--    JOIN %%DDSTG%%.ACCT_PATY_REL_THA TA
--      ON TA.TRAD_ACCT_I = AP.ACCT_I

-- WHERE (
--        (TA.EFFT_D BETWEEN AP.EFFT_D AND AP.EXPY_D)
--    OR  (AP.EFFT_D BETWEEN TA.EFFT_D AND TA.EXPY_D)
--        )


--     ) T

--GROUP BY 1,2,4,5,6,7,8,9
                        ;

     !!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.IF ERRORCODE   <> 0 THEN .GOTO EXITERR


-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '961' COLUMN '2' OF THE SOURCE CODE STARTING AT 'COLLECT'. EXPECTED 'STATEMENT' GRAMMAR. LAST MATCHING TOKEN WAS 'COLLECT' ON LINE '961' COLUMN '2'. **
-- COLLECT STATS %%DDSTG%%.DERV_ACCT_PATY_CURR
                                            ;

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.IF ERRORCODE   <> 0 THEN .GOTO EXITERR
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '973' COLUMN '2' OF THE SOURCE CODE STARTING AT 'IMPORT'. EXPECTED 'STATEMENT' GRAMMAR. LAST MATCHING TOKEN WAS 'IMPORT' ON LINE '973' COLUMN '2'. **
--                                       IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_extr_date.txt
USING
( EXTR_D VARCHAR(10) )
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '976' COLUMN '1' OF THE SOURCE CODE STARTING AT 'INSERT'. EXPECTED 'Insert' GRAMMAR. LAST MATCHING TOKEN WAS 'INTO' ON LINE '976' COLUMN '8'. **
--INSERT INTO %%DDSTG%%.DERV_ACCT_PATY_CURR
--SELECT XREF.ACCT_I
--      ,AP.PATY_I
--      ,XREF.ASSC_ACCT_I
--     ,AP.PATY_ACCT_REL_C
--     ,'N' AS PRFR_PATY_F
--     ,AP.SRCE_SYST_C

--   ,(CASE WHEN AP.EFFT_D > XREF.EFFT_D THEN AP.EFFT_D ELSE XREF.EFFT_D END) AS EFFT_D
--   ,(CASE WHEN AP.EXPY_D < XREF.EXPY_D THEN AP.EXPY_D ELSE XREF.EXPY_D END) AS EXPY_D
--   ,AP.ROW_SECU_ACCS_C
--FROM (
--                SEL ACCT_I
--                     ,PATY_I
--                     ,PATY_ACCT_REL_C
--                     ,SRCE_SYST_C
--                     ,EFFT_D
--                     ,CASE
--                               WHEN EFFT_D = EXPY_D THEN EXPY_D
--                                WHEN EXPY_D >= :EXTR_D THEN DATE'9999-12-31'
--                                 ELSE EXPY_D
--                        END AS EXPY_D
--                     ,ROW_SECU_ACCS_C
--           FROM    %%DDSTG%%.ACCT_PATY_DEDUP
--             WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D)  AP
--JOIN (
--SEL XREF1.ACCT_I
--    ,XREF2. ASSC_ACCT_I
--    ,(CASE WHEN XREF2.EFFT_D > XREF1.EFFT_D THEN XREF2.EFFT_D ELSE XREF1.EFFT_D END) AS EFFT_D
--    ,(CASE WHEN XREF2.EXPY_D < XREF1.EXPY_D THEN XREF2.EXPY_D ELSE XREF1.EXPY_D END) AS EXPY_D

--FROM (SEL ACCT_I
--                     ,SRCE_SYST_PATY_I
--                     ,SRCE_SYST_C
--                     ,EFFT_D
--                     ,CASE
--                                WHEN EFFT_D = EXPY_D THEN EXPY_D
--                                WHEN EXPY_D >= :EXTR_D THEN DATE'9999-12-31'
--                                 ELSE EXPY_D
--                        END AS EXPY_D

--           FROM   %%VTECH%%.ACCT_UNID_PATY
--           WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D)   XREF1
--JOIN (SEL ACCT_I AS ASSC_ACCT_I
--                  ,SRCE_SYST_PATY_I
--                  ,PATY_ACCT_REL_C
--                  ,SRCE_SYST_C
--                  ,ORIG_SRCE_SYST_C         -- R32 change
--                  ,EFFT_D
--                     ,CASE
--                               WHEN EFFT_D = EXPY_D THEN EXPY_D
--                                WHEN EXPY_D >= :EXTR_D THEN DATE'9999-12-31'
--                                 ELSE EXPY_D
--                        END AS EXPY_D

--           FROM   %%VTECH%%.ACCT_UNID_PATY
--           WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D)  XREF2
--  ON XREF2.SRCE_SYST_PATY_I = XREF1.SRCE_SYST_PATY_I

--JOIN %%DDSTG%%.GRD_GNRC_MAP_DERV_UNID_PATY GGM
--  ON GGM.UNID_PATY_SRCE_SYST_C = XREF2.SRCE_SYST_C
-- AND GGM.UNID_PATY_ACCT_REL_C  = XREF2.PATY_ACCT_REL_C

--   -- R32 change starts
-- AND GGM.SRCE_SYST_C  = XREF2.ORIG_SRCE_SYST_C
--  -- R32 change ends

--WHERE XREF1.SRCE_SYST_C = GGM.SRCE_SYST_C
--AND  (
--      (XREF1.EFFT_D BETWEEN XREF2.EFFT_D AND XREF2.EXPY_D)
--  OR  (XREF2.EFFT_D BETWEEN XREF1.EFFT_D AND XREF1.EXPY_D))
--     ) XREF

--  ON XREF.ASSC_ACCT_I = AP.ACCT_I
--WHERE  ((AP.EFFT_D BETWEEN XREF.EFFT_D AND XREF.EXPY_D)
--    OR  (XREF.EFFT_D BETWEEN AP.EFFT_D AND AP.EXPY_D)
--    )



--GROUP BY 1,2,3,4,5,6,7,8,9
                          ;

    !!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!

.IF ERRORCODE   <> 0 THEN .GOTO EXITERR


-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '1061' COLUMN '2' OF THE SOURCE CODE STARTING AT 'COLLECT'. EXPECTED 'STATEMENT' GRAMMAR. LAST MATCHING TOKEN WAS 'COLLECT' ON LINE '1061' COLUMN '2'. **
-- COLLECT STATS %%DDSTG%%.DERV_ACCT_PATY_CURR
                                            ;

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.IF ERRORCODE   <> 0 THEN .GOTO EXITERR
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '1074' COLUMN '2' OF THE SOURCE CODE STARTING AT 'IMPORT'. EXPECTED 'STATEMENT' GRAMMAR. LAST MATCHING TOKEN WAS 'IMPORT' ON LINE '1074' COLUMN '2'. **
--                                       IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_extr_date.txt
USING
( EXTR_D VARCHAR(10) )
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '1077' COLUMN '1' OF THE SOURCE CODE STARTING AT 'INSERT'. EXPECTED 'Insert' GRAMMAR. LAST MATCHING TOKEN WAS 'INTO' ON LINE '1077' COLUMN '8'. **
--INSERT INTO %%DDSTG%%.DERV_ACCT_PATY_CURR
--SELECT AX.ACCT_I
--       ,AP.PATY_I
--       ,AX.ASSC_ACCT_I
--       ,AP.PATY_ACCT_REL_C
--       ,'N' AS PRFR_PATY_F
--       ,AP.SRCE_SYST_C
--       ,(CASE WHEN AP.EFFT_D > AX.EFFT_D THEN AP.EFFT_D ELSE AX.EFFT_D END) AS EFFT_D
--       ,(CASE WHEN AP.EXPY_D < AX.EXPY_D THEN AP.EXPY_D ELSE AX.EXPY_D END) AS EXPY_D
--       ,AP.ROW_SECU_ACCS_C
--FROM (SEL ACCT_I
--          ,PATY_I
--          ,PATY_ACCT_REL_C
--          ,SRCE_SYST_C
--           ,EFFT_D
--           ,CASE
--               WHEN EFFT_D = EXPY_D THEN EXPY_D
--               WHEN EXPY_D >= :EXTR_D THEN DATE'9999-12-31'
--               ELSE EXPY_D
--            END AS EXPY_D
--            ,ROW_SECU_ACCS_C
--           FROM %%DDSTG%%.ACCT_PATY_DEDUP
--       WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D) AP
--JOIN
--( --start AX
--SELECT T1.LOAN_I AS ACCT_I
--       ,UIP.ACCT_I AS ASSC_ACCT_I
--        ,(CASE WHEN UIP.EFFT_D > T1.EFFT_D THEN UIP.EFFT_D ELSE T1.EFFT_D END) AS EFFT_D
--       ,(CASE WHEN UIP.EXPY_D < T1.EXPY_D THEN UIP.EXPY_D ELSE T1.EXPY_D END) AS EXPY_D
--       FROM
--(  --start T1
--SELECT LOAN.LOAN_I
--       ,FCLY.SRCE_SYST_PATY_I
--       ,(CASE WHEN LOAN.EFFT_D > FCLY.EFFT_D THEN LOAN.EFFT_D ELSE FCLY.EFFT_D END) AS EFFT_D
--       ,(CASE WHEN LOAN.EXPY_D < FCLY.EXPY_D THEN LOAN.EXPY_D ELSE FCLY.EXPY_D END) AS EXPY_D
--FROM ( SEL LOAN_I
--           ,FCLY_I
--           ,EFFT_D
--            ,CASE
--                WHEN EFFT_D = EXPY_D THEN EXPY_D
--                WHEN EXPY_D >= :EXTR_D THEN DATE'9999-12-31'
--                ELSE EXPY_D
--             END AS EXPY_D
--      FROM %%VTECH%%.MOS_LOAN
--      WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D) LOAN
--JOIN (SEL SUBSTR(FCLY_I,1,14) AS MOS_FCLY_I
--         ,SRCE_SYST_PATY_I
--         ,EFFT_D
--           ,CASE
--                WHEN EFFT_D = EXPY_D THEN EXPY_D
--                WHEN EXPY_D >= :EXTR_D THEN DATE'9999-12-31'
--                ELSE EXPY_D
--             END AS EXPY_D
--         FROM %%VTECH%%.MOS_FCLY
--        WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D) FCLY
--           ON FCLY.MOS_FCLY_I = LOAN.FCLY_I
--       WHERE (
--              (FCLY.EFFT_D BETWEEN LOAN.EFFT_D AND LOAN.EXPY_D)
--             OR (LOAN.EFFT_D BETWEEN FCLY.EFFT_D AND FCLY.EXPY_D)
--            )
-- ) T1
--JOIN (SELECT ACCT_I
--               ,SRCE_SYST_PATY_I
--               ,EFFT_D
--               ,CASE
--                  WHEN EFFT_D = EXPY_D THEN EXPY_D
--                  WHEN EXPY_D >= :EXTR_D THEN DATE'9999-12-31'
--                  ELSE EXPY_D
--                END AS EXPY_D
--           FROM %%VTECH%%.ACCT_UNID_PATY
--           WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D
--            AND SRCE_SYST_C = 'SAP'
--            AND PATY_ACCT_REL_C = 'ACTO') UIP
--ON UIP.SRCE_SYST_PATY_I = T1.SRCE_SYST_PATY_I
--WHERE (
--       (UIP.EFFT_D BETWEEN T1.EFFT_D AND T1.EXPY_D)
--        OR (T1.EFFT_D BETWEEN UIP.EFFT_D AND UIP.EXPY_D)
--      )
--) AX
--ON AX.ASSC_ACCT_I = AP.ACCT_I

--WHERE (
--       (AX.EFFT_D BETWEEN AP.EFFT_D AND AP.EXPY_D)
--        OR (AP.EFFT_D BETWEEN AX.EFFT_D AND AX.EXPY_D)
--      )



--GROUP BY 1,2,3,4,5,6,7,8,9

--UNION ALL



---- Generate rows for facility accounts

--SELECT  AX.ACCT_I
--       ,AP.PATY_I
--       ,AX.ASSC_ACCT_I
--       ,AP.PATY_ACCT_REL_C
--       ,'N' AS PRFR_PATY_F
--       ,AP.SRCE_SYST_C
--       ,(CASE WHEN AP.EFFT_D > AX.EFFT_D THEN AP.EFFT_D ELSE AX.EFFT_D END) AS EFFT_D
--       ,(CASE WHEN AP.EXPY_D < AX.EXPY_D THEN AP.EXPY_D ELSE AX.EXPY_D END) AS EXPY_D
--       ,AP.ROW_SECU_ACCS_C
--FROM (SEL ACCT_I
--         ,PATY_I
--         ,PATY_ACCT_REL_C
--         ,SRCE_SYST_C
--         ,EFFT_D
--         ,CASE
--              WHEN EFFT_D = EXPY_D THEN EXPY_D
--              WHEN EXPY_D >= :EXTR_D THEN DATE'9999-12-31'
--               ELSE EXPY_D
--          END AS EXPY_D
--          ,ROW_SECU_ACCS_C
--       FROM %%DDSTG%%.ACCT_PATY_DEDUP
--       WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D) AP
--JOIN
--(--start AX
--SELECT FCLY.FCLY_I AS ACCT_I
--       ,UIP.ACCT_I AS ASSC_ACCT_I
--       ,(CASE WHEN UIP.EFFT_D > FCLY.EFFT_D THEN UIP.EFFT_D ELSE FCLY.EFFT_D END) AS EFFT_D
--       ,(CASE WHEN UIP.EXPY_D < FCLY.EXPY_D THEN UIP.EXPY_D ELSE FCLY.EXPY_D END) AS EXPY_D
--FROM (SEL FCLY_I
--         ,SRCE_SYST_PATY_I
--         ,EFFT_D
--           ,CASE
--                WHEN EFFT_D = EXPY_D THEN EXPY_D
--                WHEN EXPY_D >= :EXTR_D THEN DATE'9999-12-31'
--                ELSE EXPY_D
--             END AS EXPY_D
--         FROM %%VTECH%%.MOS_FCLY
--        WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D) FCLY
--JOIN (SELECT ACCT_I
--               ,SRCE_SYST_PATY_I
--               ,EFFT_D
--               ,CASE
--                  WHEN EFFT_D = EXPY_D THEN EXPY_D
--                  WHEN EXPY_D >= :EXTR_D THEN DATE'9999-12-31'
--                  ELSE EXPY_D
--                END AS EXPY_D
--           FROM %%VTECH%%.ACCT_UNID_PATY
--           WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D
--            AND SRCE_SYST_C = 'SAP'
--            AND PATY_ACCT_REL_C = 'ACTO') UIP
--ON UIP.SRCE_SYST_PATY_I = FCLY.SRCE_SYST_PATY_I
--WHERE (
--       (UIP.EFFT_D BETWEEN FCLY.EFFT_D AND FCLY.EXPY_D)
--        OR (FCLY.EFFT_D BETWEEN UIP.EFFT_D AND UIP.EXPY_D)
--      )


--) AX

--ON AX.ASSC_ACCT_I = AP.ACCT_I

--WHERE (
--       (AX.EFFT_D BETWEEN AP.EFFT_D AND AP.EXPY_D)
--        OR (AP.EFFT_D BETWEEN AX.EFFT_D AND AX.EXPY_D)
--      )


--GROUP BY 1,2,3,4,5,6,7,8,9
--;
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '1242' COLUMN '1' OF THE SOURCE CODE STARTING AT '.'. EXPECTED 'STATEMENT' GRAMMAR. LAST MATCHING TOKEN WAS ';' ON LINE '1241' COLUMN '1'. FAILED TOKEN WAS '.' ON LINE '1242' COLUMN '1'. **
--.
 ;
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '1242' COLUMN '2' OF THE SOURCE CODE STARTING AT 'IF'. EXPECTED 'STATEMENT' GRAMMAR. LAST MATCHING TOKEN WAS 'IMPORT' ON LINE '1074' COLUMN '2'. FAILED TOKEN WAS 'IF' ON LINE '1242' COLUMN '2'. **
--  IF ERRORCODE   <> 0 THEN .GOTO EXITERR

--COLLECT STATS %%DDSTG%%.DERV_ACCT_PATY_CURR
                                           ;

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.IF ERRORCODE   <> 0 THEN .GOTO EXITERR

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!

.QUIT 0

-- $LastChangedBy: zakhe $
-- $LastChangedDate: 2013-11-06 12:00:30 +1100 (Wed, 06 Nov 2013) $
-- $LastChangedRevision: 12976 $
!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!

.LABEL EXITERR

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.QUIT 4