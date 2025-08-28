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
----
----  Ver  Date       Modified By        Description
----  ---- ---------- ------------------ ---------------------------------------
----  1.0  11/06/2013 T Jelliffe         Initial Version
----  1.1  11/07/2013 T Jelliffe         Use PROS_KEY_EFFT_I prevent self join
----  1.2  12/07/2013 T Jelliffe         Time period reduced 15 to 3 years
----  1.3  31/07/2013 Z Lwin             Remove EROR_SEQN_I
----  1.4  27/08/2013 T Jelliffe         Use only records with same EFFT_D
----  1.5  21/10/2013 T Jelliffe         Use Insert/Delete process
----  1.6  01/11/2013 T Jelliffe         Bug fixes - Row_Number not Rank
--------------------------------------------------------------------------------
---- This info is for CBM use only
---- $LastChangedBy: jelifft $
---- $LastChangedDate: 2013-11-06 09:17:02 +1100 (Wed, 06 Nov 2013) $
---- $LastChangedRevision: 12972 $
----

----<============================================>--
----< STEP 1 - PIG group                         >--
----<============================================>--
--!!!RESOLVE EWI!!! /*** SSC-EWI-0013 - EXCEPTION THROWN WHILE CONVERTING ITEM: Mobilize.T12Data.Sql.Ast.TdDelete. LINE: 31 OF FILE: /Users/mhindi/Library/CloudStorage/GoogleDrive-mazen.hindi@snowflake.com/My Drive/Work/Code Repos/CBA/REPO_CBA/GDW1/BTEQ/prtf_tech_paty_int_grup_psst.sql ***/!!!

--Delete
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '31' COLUMN '7' OF THE SOURCE CODE STARTING AT 'from'. EXPECTED 'FROM' GRAMMAR. LAST MATCHING TOKEN WAS 'from' ON LINE '31' COLUMN '7'. FAILED TOKEN WAS '%' ON LINE '31' COLUMN '12'. **
--       from %%STARDATADB%%.DERV_PRTF_PATY_PSST
;

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.IF ERRORCODE <> 0    THEN .GOTO EXITERR

-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '35' COLUMN '0' OF THE SOURCE CODE STARTING AT 'Insert'. EXPECTED 'Insert' GRAMMAR. LAST MATCHING TOKEN WAS 'into' ON LINE '35' COLUMN '7'. **
--Insert into %%STARDATADB%%.DERV_PRTF_PATY_PSST
--Select
--   PIG.INT_GRUP_I
--  ,PIG.PATY_I
--  ,PIG.VALD_FROM_D                   as JOIN_FROM_D
--  ,PIG.VALD_TO_D				             as JOIN_TO_D
--  ,PIG.EFFT_D
--  ,PIG.EXPY_D
--  ,PIG.VALD_FROM_D
--  ,PIG.VALD_TO_D
--  ,PIG.REL_C
--  ,PIG.SRCE_SYST_C
--  ,PIG.ROW_SECU_ACCS_C
--  ,PIG.PROS_KEY_EFFT_I                as PROS_KEY_I
--From
--  %%VTECH%%.PATY_INT_GRUP PIG

--  Inner Join %%VTECH%%.INT_GRUP IG
--  On PIG.INT_GRUP_I = IG.INT_GRUP_I
--  And (PIG.VALD_FROM_D,PIG.VALD_TO_D) Overlaps (IG.CRAT_D, IG.VALD_TO_D)

--  INNER JOIN %%VTECH%%.GRD_PRTF_TYPE_ENHC_HIST_PSST GPTE
--  ON GPTE.PRTF_TYPE_C = IG.INT_GRUP_TYPE_C
--  AND (GPTE.VALD_FROM_D,GPTE.VALD_TO_D) Overlaps (IG.CRAT_D,IG.VALD_TO_D)
--  AND (GPTE.VALD_FROM_D,GPTE.VALD_TO_D) Overlaps (PIG.VALD_FROM_D,PIG.VALD_TO_D)
--Group By 1,2,3,4,5,6,7,8,9,10,11,12
;

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.IF ERRORCODE <> 0    THEN .GOTO EXITERR

-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '64' COLUMN '1' OF THE SOURCE CODE STARTING AT 'Collect'. EXPECTED 'STATEMENT' GRAMMAR. LAST MATCHING TOKEN WAS 'Collect' ON LINE '64' COLUMN '1'. **
--Collect statistics on %%STARDATADB%%.DERV_PRTF_PATY_PSST
;

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.IF ERRORCODE <> 0    THEN .GOTO EXITERR

----<============================================>--
----< STEP 2 - Daily PIG overlaps                >--
----<============================================>--
--!!!RESOLVE EWI!!! /*** SSC-EWI-0013 - EXCEPTION THROWN WHILE CONVERTING ITEM: Mobilize.T12Data.Sql.Ast.TdDelete. LINE: 72 OF FILE: /Users/mhindi/Library/CloudStorage/GoogleDrive-mazen.hindi@snowflake.com/My Drive/Work/Code Repos/CBA/REPO_CBA/GDW1/BTEQ/prtf_tech_paty_int_grup_psst.sql ***/!!!

--Delete
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '72' COLUMN '7' OF THE SOURCE CODE STARTING AT 'from'. EXPECTED 'FROM' GRAMMAR. LAST MATCHING TOKEN WAS 'from' ON LINE '72' COLUMN '7'. FAILED TOKEN WAS '%' ON LINE '72' COLUMN '12'. **
--       from %%STARDATADB%%.DERV_PRTF_PATY_INT_GRUP_PSST All
;

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.IF ERRORCODE <> 0    THEN .GOTO EXITERR

-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '76' COLUMN '0' OF THE SOURCE CODE STARTING AT 'Insert'. EXPECTED 'Insert' GRAMMAR. LAST MATCHING TOKEN WAS 'into' ON LINE '76' COLUMN '7'. **
--Insert into %%STARDATADB%%.DERV_PRTF_PATY_INT_GRUP_PSST
--Select
--   DT2.INT_GRUP_I
--  ,DT2.PATY_I
--  ,DT2.EFFT_D
--  ,DT2.EXPY_D
--  ,DT2.VALD_FROM_D
--  ,DT2.VALD_TO_D
--  ,DT2.PERD_D
--  ,DT2.REL_C
--  ,DT2.SRCE_SYST_C
--	,ROW_NUMBER() OVER (Partition By DT2.INT_GRUP_I, DT2.PATY_I Order by DT2.PERD_D, DT2.REL_C ) as GRUP_N
--  ,DT2.ROW_SECU_ACCS_C
--  ,DT2.PROS_KEY_I
--From
--  (
--    Select
--       A.INT_GRUP_I
--      ,A.PATY_I
--      ,C.CALENDAR_DATE				as PERD_D
--      ,A.EFFT_D
--      ,A.EXPY_D
--      ,A.VALD_FROM_D
--      ,A.VALD_TO_D
--      ,A.REL_C
--      ,A.SRCE_SYST_C
--      ,A.ROW_SECU_ACCS_C
--      ,A.PROS_KEY_I
--    From
--      %%VTECH%%.DERV_PRTF_PATY_PSST A
--      Inner Join %%VTECH%%.DERV_PRTF_PATY_PSST B
--      On A.PATY_I = B.PATY_I
--      And A.INT_GRUP_I = B.INT_GRUP_I
--      And (A.JOIN_FROM_D,A.JOIN_TO_D) overlaps (B.JOIN_FROM_D,B.JOIN_TO_D)
--      And (
--        A.JOIN_FROM_D <> B.JOIN_FROM_D
--        Or A.JOIN_TO_D <> B.JOIN_TO_D
--      )

--      Inner Join %%VTECH%%.CALENDAR C
--      On C.CALENDAR_DATE between A.VALD_FROM_D and A.VALD_TO_D
--      And C.CALENDAR_DATE between ADD_MONTHS((Current_Date - EXTRACT(DAY from CURRENT_DATE) +1 ),-39) and ADD_MONTHS(Current_Date, 1)

--    Qualify Row_Number() Over( Partition By A.PATY_I, A.INT_GRUP_I, C.CALENDAR_DATE Order BY A.EFFT_D Desc) = 1
--    Group By 1,2,3,4,5,6,7,8,9,10,11
--  ) DT2
;

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.IF ERRORCODE <> 0    THEN .GOTO EXITERR

-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '125' COLUMN '1' OF THE SOURCE CODE STARTING AT 'Collect'. EXPECTED 'STATEMENT' GRAMMAR. LAST MATCHING TOKEN WAS 'Collect' ON LINE '125' COLUMN '1'. **
--Collect Statistics on %%STARDATADB%%.DERV_PRTF_PATY_INT_GRUP_PSST
                                                                 ;

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.IF ERRORCODE <> 0    THEN .GOTO EXITERR


----<============================================>--
----< STEP 3 - History group                     >--
----<============================================>--
--!!!RESOLVE EWI!!! /*** SSC-EWI-0013 - EXCEPTION THROWN WHILE CONVERTING ITEM: Mobilize.T12Data.Sql.Ast.TdDelete. LINE: 132 OF FILE: /Users/mhindi/Library/CloudStorage/GoogleDrive-mazen.hindi@snowflake.com/My Drive/Work/Code Repos/CBA/REPO_CBA/GDW1/BTEQ/prtf_tech_paty_int_grup_psst.sql ***/!!!
--Delete
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '132' COLUMN '7' OF THE SOURCE CODE STARTING AT 'from'. EXPECTED 'FROM' GRAMMAR. LAST MATCHING TOKEN WAS 'from' ON LINE '132' COLUMN '7'. FAILED TOKEN WAS '%' ON LINE '132' COLUMN '12'. **
--       from %%STARDATADB%%.DERV_PRTF_PATY_HIST_PSST All
;

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.IF ERRORCODE <> 0    THEN .GOTO EXITERR


-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '137' COLUMN '0' OF THE SOURCE CODE STARTING AT 'Insert'. EXPECTED 'Insert' GRAMMAR. LAST MATCHING TOKEN WAS 'into' ON LINE '137' COLUMN '7'. **
--Insert into %%STARDATADB%%.DERV_PRTF_PATY_HIST_PSST
--Select Distinct
--   DT2.INT_GRUP_I
--  ,DT2.PATY_I
--  ,DT2.REL_C
--  ,MIN(DT2.PERD_D) Over ( Partition By DT2.PATY_I, DT2.INT_GRUP_I, DT2.GRUP_N ) as JOIN_FROM_D
--  ,MAX(DT2.PERD_D) Over ( Partition By DT2.PATY_I, DT2.INT_GRUP_I, DT2.GRUP_N ) as JOIN_TO_D
--  ,DT2.VALD_FROM_D
--  ,DT2.VALD_TO_D
--  ,DT2.EFFT_D
--  ,DT2.EXPY_D
--  ,DT2.SRCE_SYST_C
--  ,DT2.ROW_SECU_ACCS_C
--  ,DT2.PROS_KEY_I
--From
--  (
--  Select
--     C.INT_GRUP_I
--    ,C.PATY_I
--    ,C.REL_C
--    ,C.VALD_FROM_D
--    ,C.VALD_TO_D
--    ,C.EFFT_D
--    ,C.EXPY_D
--    ,C.SRCE_SYST_C
--    ,C.ROW_SECU_ACCS_C
--    ,C.PERD_D
--    ,MAX(Coalesce( DT1.ROW_N, 0 )) Over (Partition By C.INT_GRUP_I, C.PATY_I, C.REL_C, C.PERD_D) as GRUP_N
--    ,C.PROS_KEY_I
--  From
--    %%VTECH%%.DERV_PRTF_PATY_INT_GRUP_PSST C
--    Left Join (
--      -- Detect the change in non-key values between rows
--      Select
--         A.INT_GRUP_I
--        ,A.PATY_I
--        ,A.EFFT_D
--        ,A.REL_C
--        ,A.ROW_N
--        ,A.PERD_D
--      From
--        %%VTECH%%.DERV_PRTF_PATY_INT_GRUP_PSST A
--        Inner Join %%VTECH%%.DERV_PRTF_PATY_INT_GRUP_PSST B
--        On A.PATY_I = B.PATY_I
--        And A.INT_GRUP_I = B.INT_GRUP_I
--        And A.ROW_N = B.ROW_N + 1
--        And A.EFFT_D <> B.EFFT_D
--    ) DT1
--    On C.INT_GRUP_I = DT1.INT_GRUP_I
--    And C.PATY_I = DT1.PATY_I
--    And C.INT_GRUP_I = DT1.INT_GRUP_I
--    And C.REL_C = DT1.REL_C
--    And C.EFFT_D = DT1.EFFT_D
--    And C.ROW_N >= DT1.ROW_N
--    And DT1.PERD_D <= C.PERD_D

--  ) DT2
;

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.IF ERRORCODE <> 0    THEN .GOTO EXITERR

-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '197' COLUMN '1' OF THE SOURCE CODE STARTING AT 'Collect'. EXPECTED 'STATEMENT' GRAMMAR. LAST MATCHING TOKEN WAS 'Collect' ON LINE '197' COLUMN '1'. **
--Collect Statistics on %%STARDATADB%%.DERV_PRTF_PATY_HIST_PSST
                                                             ;

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.IF ERRORCODE <> 0    THEN .GOTO EXITERR


----<================================================>--
----< STEP 4 Delete all the original overlap records >--
----<================================================>--
--!!!RESOLVE EWI!!! /*** SSC-EWI-0013 - EXCEPTION THROWN WHILE CONVERTING ITEM: Mobilize.T12Data.Sql.Ast.TdDelete. LINE: 205 OF FILE: /Users/mhindi/Library/CloudStorage/GoogleDrive-mazen.hindi@snowflake.com/My Drive/Work/Code Repos/CBA/REPO_CBA/GDW1/BTEQ/prtf_tech_paty_int_grup_psst.sql ***/!!!

--Delete A
---- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '207' COLUMN '1' OF THE SOURCE CODE STARTING AT 'From'. EXPECTED 'FROM' GRAMMAR. LAST MATCHING TOKEN WAS 'From' ON LINE '207' COLUMN '1'. FAILED TOKEN WAS '%' ON LINE '208' COLUMN '3'. **
--From
--	 %%STARDATADB%%.DERV_PRTF_PATY_PSST A
--	,%%VTECH%%.DERV_PRTF_PATY_HIST_PSST B
--Where
--	A.PATY_I = B.PATY_I
--  And A.INT_GRUP_I = B.INT_GRUP_I
--  And PUBLIC.PERIOD_OVERLAPS_UDF(ARRAY_CONSTRUCT( (A.JOIN_FROM_D, A.JOIN_TO_D), (B.JOIN_FROM_D,B.JOIN_TO_D))) !!!RESOLVE EWI!!! /*** SSC-EWI-TD0053 - SNOWFLAKE DOES NOT SUPPORT THE PERIOD DATATYPE, ALL PERIODS ARE HANDLED AS VARCHAR INSTEAD ***/!!!
;

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.IF ERRORCODE <> 0    THEN .GOTO EXITERR



-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '223' COLUMN '0' OF THE SOURCE CODE STARTING AT 'Insert'. EXPECTED 'Insert' GRAMMAR. LAST MATCHING TOKEN WAS 'into' ON LINE '223' COLUMN '7'. **
----<================================================>--
----< STEP 5 - Replace with updated records          >--
----<================================================>--

--Insert into %%STARDATADB%%.DERV_PRTF_PATY_PSST
--Select
--   INT_GRUP_I
--  ,PATY_I
--  ,JOIN_FROM_D
--  ,(Case
--      WHEN JOIN_TO_D = ADD_MONTHS(CURRENT_DATE, 1) Then ('9999-12-31'(Date,format'yyyy-mm-dd'))
--      ELSE JOIN_TO_D
--    End
--    ) as JOIN_TO_D
--  ,EFFT_D
--  ,EXPY_D
--  ,VALD_FROM_D
--  ,VALD_TO_D
--  ,REL_C
--  ,SRCE_SYST_C
--  ,ROW_SECU_ACCS_C
--  ,PROS_KEY_I
--From
--  %%VTECH%%.DERV_PRTF_PATY_HIST_PSST
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