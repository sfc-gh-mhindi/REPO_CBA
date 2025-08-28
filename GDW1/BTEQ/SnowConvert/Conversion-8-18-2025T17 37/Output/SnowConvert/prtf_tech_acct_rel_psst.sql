!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.RUN FILE=%%BTEQ_LOGON_SCRIPT%%

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.IF ERRORCODE <> 0    THEN .GOTO EXITERR

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!

.SET QUIET OFF

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.SET ECHOREQ ON

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.SET FORMAT OFF

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.SET WIDTH 120

--------------------------------------------------------------------------------
---- Object Name             :  prtf_tech_acct_rel_psst.sql
---- Object Type             :  BTEQ
----                           
---- Description             :  Persist rows for DERV_PRTF_ACCT view

--------------------------------------------------------------------------------
----   Modification history

-------------------------------------------------------------------------------- 
----  Ver  Date             Modified By        Description
----  ---- ------------    ------------       ----------------------------------
----  1.0  09/01/2014       Helen Zak          Initial Version
----  2.0  07/02/2014       Zeewa Lwin         Remove VALD_FROM_D and VALD_TO_D
----                                           values extracting from GRD table
--------------------------------------------------------------------------------
---- This info is for CBM use only
---- $LastChangedBy: lwinze $
---- $LastChangedDate: 2014-02-11 16:12:19 +1100 (Tue, 11 Feb 2014) $
---- $LastChangedRevision: 13210 $
----
--!!!RESOLVE EWI!!! /*** SSC-EWI-0013 - EXCEPTION THROWN WHILE CONVERTING ITEM: Mobilize.T12Data.Sql.Ast.TdDelete. LINE: 32 OF FILE: /Users/mhindi/Library/CloudStorage/GoogleDrive-mazen.hindi@snowflake.com/My Drive/Work/Code Repos/CBA/REPO_CBA/GDW1/BTEQ/prtf_tech_acct_rel_psst.sql ***/!!!


--Delete
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '32' COLUMN '7' OF THE SOURCE CODE STARTING AT 'from'. EXPECTED 'FROM' GRAMMAR. LAST MATCHING TOKEN WAS 'from' ON LINE '32' COLUMN '7'. FAILED TOKEN WAS '%' ON LINE '32' COLUMN '12'. **
--       from %%STARDATADB%%.DERV_PRTF_ACCT_REL All
;

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.IF ERRORCODE <> 0    THEN .GOTO EXITERR

-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '38' COLUMN '2' OF THE SOURCE CODE STARTING AT 'COLLECT'. EXPECTED 'STATEMENT' GRAMMAR. LAST MATCHING TOKEN WAS 'COLLECT' ON LINE '38' COLUMN '2'. **
---- Collect stats after deletion so that the optimiser "knows" that the table is empty

-- COLLECT STATS  %%STARDATADB%%.DERV_PRTF_ACCT_REL
                                                 ;

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!

.IF ERRORCODE <> 0    THEN .GOTO EXITERR

-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '42' COLUMN '0' OF THE SOURCE CODE STARTING AT 'Insert'. EXPECTED 'Insert' GRAMMAR. LAST MATCHING TOKEN WAS 'into' ON LINE '42' COLUMN '7'. **
--Insert into %%STARDATADB%%.DERV_PRTF_ACCT_REL
--(
--       ACCT_I
--      ,INT_GRUP_I
--      ,DERV_PRTF_CATG_C
--      ,DERV_PRTF_CLAS_C
--      ,DERV_PRTF_TYPE_C
--      ,VALD_FROM_D
--      ,VALD_TO_D
--      ,EFFT_D
--      ,EXPY_D
--      ,PTCL_N
--      ,REL_MNGE_I
--      ,PRTF_CODE_X
--      ,SRCE_SYST_C
--      ,ROW_SECU_ACCS_C
--)
--Select
--   DT1.ACCT_I
--  ,DT1.INT_GRUP_I
--  ,GPTE2.PRTF_CATG_C                            AS DERV_PRTF_CATG_C
--  ,GPTE2.PRTF_CLAS_C                            AS DERV_PRTF_CLAS_C
--  ,DT1.DERV_PRTF_TYPE_C
--  ,DT1.VALD_FROM_D
--  ,DT1.VALD_TO_D
--  ,DT1.EFFT_D
--  ,DT1.EXPY_D
--  ,DT1.PTCL_N
--  ,DT1.REL_MNGE_I
--  ,DT1.PRTF_CODE_X
--  ,DT1.SRCE_SYST_C
--  ,DT1.ROW_SECU_ACCS_C
--From
--  (
--    Select
--       PIG3.ACCT_I                                  AS ACCT_I
--      ,IG3.INT_GRUP_I                               AS INT_GRUP_I
--      ,PIG3.EFFT_D                                  AS EFFT_D
--      ,PIG3.EXPY_D                                  AS EXPY_D
--      ,IG3.INT_GRUP_TYPE_C                          AS DERV_PRTF_TYPE_C
--      ,CAST(IG3.PTCL_N AS SMALLINT)                 AS PTCL_N
--      ,IG3.REL_MNGE_I                               AS REL_MNGE_I
--      ,(CASE
--          WHEN (IG3.PTCL_N IS NULL) OR (IG3.REL_MNGE_I IS NULL) THEN 'NA'
--          ELSE TRIM(IG3.PTCL_N) || TRIM(IG3.REL_MNGE_I)
--        END)                                        AS PRTF_CODE_X
--      ,PIG3.SRCE_SYST_C                             AS SRCE_SYST_C
--      ,PIG3.ROW_SECU_ACCS_C                         AS ROW_SECU_ACCS_C
--      ,(CASE
--          WHEN IG3.JOIN_FROM_D > PIG3.JOIN_FROM_D THEN IG3.JOIN_FROM_D
--          ELSE PIG3.JOIN_FROM_D
--        END) as VALD_FROM_D
--      ,(CASE
--          WHEN IG3.JOIN_TO_D < PIG3.JOIN_TO_D Then IG3.JOIN_TO_D
--          ELSE PIG3.JOIN_TO_D
--        END ) as VALD_TO_D
--    FROM
--      %%VTECH%%.DERV_PRTF_ACCT_PSST PIG3
--      INNER JOIN %%VTECH%%.DERV_PRTF_INT_PSST IG3
--      ON IG3.INT_GRUP_I = PIG3.INT_GRUP_I
--      AND PIG3.JOIN_TO_D >= IG3.JOIN_FROM_D
--      AND PIG3.JOIN_FROM_D <= IG3.JOIN_TO_D
--  ) DT1

--  INNER JOIN %%VTECH%%.GRD_PRTF_TYPE_ENHC_HIST_PSST GPTE2
--  ON GPTE2.PRTF_TYPE_C = DT1.DERV_PRTF_TYPE_C
;

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!

.IF ERRORCODE <> 0    THEN .GOTO EXITERR

-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '112' COLUMN '1' OF THE SOURCE CODE STARTING AT 'COLLECT'. EXPECTED 'STATEMENT' GRAMMAR. LAST MATCHING TOKEN WAS 'COLLECT' ON LINE '112' COLUMN '1'. **
--COLLECT STATS  %%STARDATADB%%.DERV_PRTF_ACCT_REL
                                                ;

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!

.IF ERRORCODE <> 0    THEN .GOTO EXITERR

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!


.QUIT 0

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.LOGOFF

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!

.LABEL EXITERR

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.QUIT 1

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.LOGOFF

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.EXIT