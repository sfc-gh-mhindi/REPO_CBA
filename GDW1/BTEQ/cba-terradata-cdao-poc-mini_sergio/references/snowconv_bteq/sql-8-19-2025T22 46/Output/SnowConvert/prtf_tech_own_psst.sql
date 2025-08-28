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
----  1.0  18/07/2013 T Jelliffe         Initial Version
----  1.1  01/11/2013 T Jelliffe         Bug fixes - Row_Number not Rank
----  1.2  27/11/2013 T Jelliffe         Fix overlap calc, JOIN_DATE to calendar
--------------------------------------------------------------------------------
---- This info is for CBM use only
---- $LastChangedBy: jelifft $
---- $LastChangedDate: 2013-11-27 14:06:55 +1100 (Wed, 27 Nov 2013) $
---- $LastChangedRevision: 13104 $
----


----<=========================================================================>--
----< Step 5 : Rule 3 & 4 - Only one Dept/Empl owner
----<=========================================================================>--
--!!!RESOLVE EWI!!! /*** SSC-EWI-0013 - EXCEPTION THROWN WHILE CONVERTING ITEM: Mobilize.T12Data.Sql.Ast.TdDelete. LINE: 27 OF FILE: /Users/psundaram/Documents/prjs/cba/agentic/bteq/prtf_tech_own_psst.sql ***/!!!
--Delete
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '27' COLUMN '7' OF THE SOURCE CODE STARTING AT 'from'. EXPECTED 'FROM' GRAMMAR. LAST MATCHING TOKEN WAS 'from' ON LINE '27' COLUMN '7'. FAILED TOKEN WAS '%' ON LINE '27' COLUMN '12'. **
--       from %%STARDATADB%%.DERV_PRTF_INT_GRUP_OWN_PSST All
;

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.IF ERRORCODE <> 0    THEN .GOTO EXITERR

-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '31' COLUMN '0' OF THE SOURCE CODE STARTING AT 'Insert'. EXPECTED 'Insert' GRAMMAR. LAST MATCHING TOKEN WAS 'into' ON LINE '31' COLUMN '7'. **
--Insert into %%STARDATADB%%.DERV_PRTF_INT_GRUP_OWN_PSST
--Select
--   DT2.INT_GRUP_I           as INT_GRUP_I
--  ,DT2.ROLE_PLAY_I          as ROLE_PLAY_I
--  ,DT2.EFFT_D               as EFFT_D
--  ,DT2.EXPY_D               as EXPY_D
--  ,DT2.VALD_FROM_D          as VALD_FROM_D
--  ,DT2.VALD_TO_D            as VALD_TO_D
--  ,DT2.PERD_D			          as PERD_D
--  ,DT2.ROLE_PLAY_TYPE_X     as ROLE_PLAY_TYPE_X
--  ,DT2.DERV_PRTF_ROLE_C     as DERV_PRTF_ROLE_C
--  ,DT2.SRCE_SYST_C          as SRCE_SYST_C
--	,ROW_NUMBER() OVER (Partition By DT2.INT_GRUP_I Order by DT2.PERD_D, DT2.DERV_PRTF_ROLE_C )
--  ,DT2.ROW_SECU_ACCS_C      as ROW_SECU_ACCS_C
--  ,0                        as PROS_KEY_I
--From
--  (
--     -- KEY =  (INT_GRUP_I, DERV_PRTF_ROLE_C)
--    Select
--       A.INT_GRUP_I           as INT_GRUP_I
--      ,A.ROLE_PLAY_I               as ROLE_PLAY_I
--      ,C.CALENDAR_DATE as PERD_D
--      ,A.EFFT_D               as EFFT_D
--      ,A.EXPY_D               as EXPY_D
--      ,A.ROLE_PLAY_TYPE_X     as ROLE_PLAY_TYPE_X
--      ,A.DERV_PRTF_ROLE_C     as DERV_PRTF_ROLE_C
--      ,A.VALD_FROM_D
--      ,A.VALD_TO_D
--      ,A.SRCE_SYST_C          as SRCE_SYST_C
--      ,A.ROW_SECU_ACCS_C      as ROW_SECU_ACCS_C
--    From
--      %%VTECH%%.DERV_PRTF_OWN_PSST A
--      Inner Join %%VTECH%%.DERV_PRTF_OWN_PSST B
--      On A.INT_GRUP_I = B.INT_GRUP_I
--      And A.DERV_PRTF_ROLE_C = B.DERV_PRTF_ROLE_C
--      --And A.ROLE_PLAY_I <> B.ROLE_PLAY_I -- Same ROLE_PLAY_I already in step 1-4.
--      And A.DERV_PRTF_ROLE_C = 'OWNR'
--      And A.ROLE_PLAY_TYPE_X = B.ROLE_PLAY_TYPE_X -- Compare Empl to Empl and Dept to Dept only. What about ensuring that rule 5, use EMPL first!!
--      -- Rule 5 seperate step, as the INT_GRUP_I values will be different
--      And (A.JOIN_FROM_D,A.JOIN_TO_D) overlaps (B.JOIN_FROM_D,B.JOIN_TO_D)
--      And (
--      	A.JOIN_FROM_D <> B.JOIN_FROM_D
--      	Or A.JOIN_TO_D <> B.JOIN_TO_D
--        Or A.ROLE_PLAY_I <> B.ROLE_PLAY_I
--      )

--			Inner Join %%VTECH%%.CALENDAR C
--			On C.CALENDAR_DATE between A.JOIN_FROM_D and A.JOIN_TO_D
--			And C.CALENDAR_DATE between ADD_MONTHS((Current_Date - EXTRACT(DAY from CURRENT_DATE) +1 ),-39) and ADD_MONTHS(Current_Date, 1)

--		Qualify Row_Number() Over( Partition By A.INT_GRUP_I, A.ROLE_PLAY_TYPE_X, C.CALENDAR_DATE Order BY A.EFFT_D Desc, A.ROLE_PLAY_I Desc ) = 1
--	) DT2
;

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.IF ERRORCODE <> 0    THEN .GOTO EXITERR

-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '86' COLUMN '1' OF THE SOURCE CODE STARTING AT 'Collect'. EXPECTED 'STATEMENT' GRAMMAR. LAST MATCHING TOKEN WAS 'Collect' ON LINE '86' COLUMN '1'. **
--Collect Statistics on %%STARDATADB%%.DERV_PRTF_INT_GRUP_OWN_PSST
                                                                ;

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.IF ERRORCODE <> 0    THEN .GOTO EXITERR

----<=========================================================================>--
----< Step 6 : Hist view of Step 5
----<=========================================================================>--
--!!!RESOLVE EWI!!! /*** SSC-EWI-0013 - EXCEPTION THROWN WHILE CONVERTING ITEM: Mobilize.T12Data.Sql.Ast.TdDelete. LINE: 92 OF FILE: /Users/psundaram/Documents/prjs/cba/agentic/bteq/prtf_tech_own_psst.sql ***/!!!
--Delete
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '92' COLUMN '7' OF THE SOURCE CODE STARTING AT 'from'. EXPECTED 'FROM' GRAMMAR. LAST MATCHING TOKEN WAS 'from' ON LINE '92' COLUMN '7'. FAILED TOKEN WAS '%' ON LINE '92' COLUMN '12'. **
--       from %%STARDATADB%%.DERV_PRTF_OWN_HIST_PSST
;

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.IF ERRORCODE <> 0    THEN .GOTO EXITERR

-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '96' COLUMN '0' OF THE SOURCE CODE STARTING AT 'Insert'. EXPECTED 'Insert' GRAMMAR. LAST MATCHING TOKEN WAS 'into' ON LINE '96' COLUMN '7'. **
--Insert into %%STARDATADB%%.DERV_PRTF_OWN_HIST_PSST
--Select Distinct
--	 DT2.INT_GRUP_I
--	,DT2.ROLE_PLAY_I
--  ,MIN(DT2.PERD_D) Over ( Partition By DT2.INT_GRUP_I, DT2.ROLE_PLAY_I, DT2.GRUP_N ) as JOIN_FROM_D
--  ,MAX(DT2.PERD_D) Over ( Partition By DT2.INT_GRUP_I, DT2.ROLE_PLAY_I, DT2.GRUP_N ) as JOIN_TO_D
--	,DT2.EFFT_D
--	,DT2.EXPY_D
--	,DT2.VALD_FROM_D
--	,DT2.VALD_TO_D
--  ,DT2.ROLE_PLAY_TYPE_X
--	,DT2.DERV_PRTF_ROLE_C
--	,DT2.SRCE_SYST_C
--	,DT2.ROW_SECU_ACCS_C
--	,DT2.PROS_KEY_I
--From
--  (
--		Select Distinct
--			 C.INT_GRUP_I
--			,C.ROLE_PLAY_I
--			,C.EFFT_D
--			,C.EXPY_D
--			,C.VALD_FROM_D
--			,C.VALD_TO_D
--			,C.PERD_D
--			,C.ROLE_PLAY_TYPE_X
--			,C.DERV_PRTF_ROLE_C
--			,C.SRCE_SYST_C
--			,C.ROW_N
--			,C.ROW_SECU_ACCS_C
--			,C.PROS_KEY_I
--      ,MAX(Coalesce( DT1.ROW_N, 0 )) Over (Partition By C.INT_GRUP_I, C.ROLE_PLAY_I, C.PERD_D) as GRUP_N
--		From
--			%%VTECH%%.DERV_PRTF_INT_GRUP_OWN_PSST C
--			Left Join (
--				-- Detect the change in non-key values between rows
--				Select
--           A.INT_GRUP_I
--          ,A.ROLE_PLAY_I
--          ,A.DERV_PRTF_ROLE_C
--          ,A.ROLE_PLAY_TYPE_X
--          ,A.ROW_N
--          ,A.PERD_D
--          ,A.EFFT_D
--				From
--					%%VTECH%%.DERV_PRTF_INT_GRUP_OWN_PSST A
--					Inner Join %%VTECH%%.DERV_PRTF_INT_GRUP_OWN_PSST B

--					On A.INT_GRUP_I = B.INT_GRUP_I
--					And A.ROW_N = B.ROW_N + 1
--					And A.DERV_PRTF_ROLE_C = B.DERV_PRTF_ROLE_C
--          And A.DERV_PRTF_ROLE_C = 'OWNR'
--          And A.ROLE_PLAY_TYPE_X = B.ROLE_PLAY_TYPE_X
--          And (
--            A.ROLE_PLAY_I <> B.ROLE_PLAY_I
--  					Or  A.EFFT_D <> B.EFFT_D
--					)

--			) DT1
--			On C.INT_GRUP_I = DT1.INT_GRUP_I
--			And C.ROLE_PLAY_TYPE_X = DT1.ROLE_PLAY_TYPE_X
--			And C.EFFT_D = DT1.EFFT_D
--      And C.ROW_N >= DT1.ROW_N
--      And DT1.PERD_D <= C.PERD_D

--  ) DT2
;

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.IF ERRORCODE <> 0    THEN .GOTO EXITERR

-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '165' COLUMN '1' OF THE SOURCE CODE STARTING AT 'Collect'. EXPECTED 'STATEMENT' GRAMMAR. LAST MATCHING TOKEN WAS 'Collect' ON LINE '165' COLUMN '1'. **
--Collect Statistics on %%STARDATADB%%.DERV_PRTF_OWN_HIST_PSST
                                                            ;

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.IF ERRORCODE <> 0    THEN .GOTO EXITERR


----<=========================================================================>--
----< Step 7 : Delete and refresh for Step 6
----<=========================================================================>--
--!!!RESOLVE EWI!!! /*** SSC-EWI-0013 - EXCEPTION THROWN WHILE CONVERTING ITEM: Mobilize.T12Data.Sql.Ast.TdDelete. LINE: 173 OF FILE: /Users/psundaram/Documents/prjs/cba/agentic/bteq/prtf_tech_own_psst.sql ***/!!!

--Delete A
---- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '175' COLUMN '1' OF THE SOURCE CODE STARTING AT 'From'. EXPECTED 'FROM' GRAMMAR. LAST MATCHING TOKEN WAS 'From' ON LINE '175' COLUMN '1'. FAILED TOKEN WAS '%' ON LINE '176' COLUMN '3'. **
--From
--	 %%STARDATADB%%.DERV_PRTF_OWN_PSST A
--	,%%VTECH%%.DERV_PRTF_OWN_HIST_PSST B
--Where
--	A.INT_GRUP_I = B.INT_GRUP_I
--  And A.ROLE_PLAY_TYPE_X = B.ROLE_PLAY_TYPE_X
--  And A.DERV_PRTF_ROLE_C = B.DERV_PRTF_ROLE_C
--  And PUBLIC.PERIOD_OVERLAPS_UDF(ARRAY_CONSTRUCT( (A.JOIN_FROM_D, A.JOIN_TO_D), (B.JOIN_FROM_D,B.JOIN_TO_D))) !!!RESOLVE EWI!!! /*** SSC-EWI-TD0053 - SNOWFLAKE DOES NOT SUPPORT THE PERIOD DATATYPE, ALL PERIODS ARE HANDLED AS VARCHAR INSTEAD ***/!!!
;

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.IF ERRORCODE <> 0    THEN .GOTO EXITERR

-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '186' COLUMN '0' OF THE SOURCE CODE STARTING AT 'Insert'. EXPECTED 'Insert' GRAMMAR. LAST MATCHING TOKEN WAS 'into' ON LINE '186' COLUMN '7'. **
--Insert into %%STARDATADB%%.DERV_PRTF_OWN_PSST
--Select
--   INT_GRUP_I
--  ,JOIN_FROM_D
--  ,(Case
--      WHEN JOIN_TO_D = ADD_MONTHS(CURRENT_DATE, 1) Then ('9999-12-31'(Date,format'yyyy-mm-dd'))
--      ELSE JOIN_TO_D
--    End
--    ) as JOIN_TO_D
--  ,VALD_FROM_D
--  ,VALD_TO_D
--  ,EFFT_D
--  ,EXPY_D
--  ,DERV_PRTF_ROLE_C
--  ,ROLE_PLAY_TYPE_X
--  ,ROLE_PLAY_I
--  ,SRCE_SYST_C
--  ,ROW_SECU_ACCS_C
--  ,PROS_KEY_I
--From
--  %%VTECH%%.DERV_PRTF_OWN_HIST_PSST
;

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.IF ERRORCODE <> 0    THEN .GOTO EXITERR

----<=========================================================================>--
----< Step 8 : When both Dept and Empl take Empl (Rule 5)
----<=========================================================================>--
--!!!RESOLVE EWI!!! /*** SSC-EWI-0013 - EXCEPTION THROWN WHILE CONVERTING ITEM: Mobilize.T12Data.Sql.Ast.TdDelete. LINE: 213 OF FILE: /Users/psundaram/Documents/prjs/cba/agentic/bteq/prtf_tech_own_psst.sql ***/!!!
--Delete
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '213' COLUMN '7' OF THE SOURCE CODE STARTING AT 'from'. EXPECTED 'FROM' GRAMMAR. LAST MATCHING TOKEN WAS 'from' ON LINE '213' COLUMN '7'. FAILED TOKEN WAS '%' ON LINE '213' COLUMN '12'. **
--       from %%STARDATADB%%.DERV_PRTF_INT_GRUP_OWN_PSST All
;

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.IF ERRORCODE <> 0    THEN .GOTO EXITERR

-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '217' COLUMN '0' OF THE SOURCE CODE STARTING AT 'Insert'. EXPECTED 'Insert' GRAMMAR. LAST MATCHING TOKEN WAS 'into' ON LINE '217' COLUMN '7'. **
--Insert into %%STARDATADB%%.DERV_PRTF_INT_GRUP_OWN_PSST
--Select
--   DT2.INT_GRUP_I           as INT_GRUP_I
--  ,DT2.ROLE_PLAY_I          as ROLE_PLAY_I
--  ,DT2.EFFT_D               as EFFT_D
--  ,DT2.EXPY_D               as EXPY_D
--  ,DT2.VALD_FROM_D          as VALD_FROM_D
--  ,DT2.VALD_TO_D            as VALD_TO_D
--  ,DT2.PERD_D				        as PERD_D
--  ,DT2.ROLE_PLAY_TYPE_X     as ROLE_PLAY_TYPE_X
--  ,DT2.DERV_PRTF_ROLE_C     as DERV_PRTF_ROLE_C
--  ,DT2.SRCE_SYST_C          as SRCE_SYST_C
--	,ROW_NUMBER() OVER (Partition By DT2.INT_GRUP_I, DT2.DERV_PRTF_ROLE_C Order by  DT2.PERD_D, DT2.ROLE_PLAY_I Desc)
--  ,DT2.ROW_SECU_ACCS_C      as ROW_SECU_ACCS_C
--  ,0                        as PROS_KEY_I
--From
--	(
--    Select
--       A.INT_GRUP_I           as INT_GRUP_I
--      ,A.ROLE_PLAY_I
--      ,A.DERV_PRTF_ROLE_C
--      ,C.CALENDAR_DATE as PERD_D
--      ,A.EFFT_D               as EFFT_D
--      ,A.EXPY_D               as EXPY_D
--      ,A.ROLE_PLAY_TYPE_X --'Employee'             
--      ,A.VALD_FROM_D
--      ,A.VALD_TO_D
--      ,A.SRCE_SYST_C          as SRCE_SYST_C
--      ,A.ROW_SECU_ACCS_C      as ROW_SECU_ACCS_C
--    From
--      /* KEY = (INT_GRUP_I, ROLE_PLAY_I) */
--      %%VTECH%%.DERV_PRTF_OWN_PSST A
--      Inner Join %%VTECH%%.DERV_PRTF_OWN_PSST B

--      On A.INT_GRUP_I = B.INT_GRUP_I
--      And A.DERV_PRTF_ROLE_C = B.DERV_PRTF_ROLE_C
--      And A.DERV_PRTF_ROLE_C = 'OWNR'
--      And A.ROLE_PLAY_TYPE_X <> B.ROLE_PLAY_TYPE_X
--      And (A.JOIN_FROM_D,A.JOIN_TO_D) overlaps (B.JOIN_FROM_D,B.JOIN_TO_D)
--      --And (
--      --	A.JOIN_FROM_D <> B.JOIN_FROM_D
--      --	Or A.JOIN_TO_D <> B.JOIN_TO_D
--      --)   

--      Inner Join %%VTECH%%.CALENDAR C
--      --On C.CALENDAR_DATE between A.VALD_FROM_D and A.VALD_TO_D
--      On C.CALENDAR_DATE between A.JOIN_FROM_D and A.JOIN_TO_D
--      And C.CALENDAR_DATE between ADD_MONTHS((Current_Date - EXTRACT(DAY from CURRENT_DATE) +1 ),-39) and ADD_MONTHS(Current_Date, 1)

--    Qualify Row_Number() Over( Partition By A.INT_GRUP_I, A.DERV_PRTF_ROLE_C, C.CALENDAR_DATE Order BY A.EFFT_D Desc, A.ROLE_PLAY_TYPE_X Desc, A.ROLE_PLAY_I Desc ) = 1
--    Group By 1,2,3,4,5,6,7,8,9,10,11
--	) DT2
;

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.IF ERRORCODE <> 0    THEN .GOTO EXITERR

-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '272' COLUMN '1' OF THE SOURCE CODE STARTING AT 'Collect'. EXPECTED 'STATEMENT' GRAMMAR. LAST MATCHING TOKEN WAS 'Collect' ON LINE '272' COLUMN '1'. **
--Collect Statistics on %%STARDATADB%%.DERV_PRTF_INT_GRUP_OWN_PSST
                                                                ;

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.IF ERRORCODE <> 0    THEN .GOTO EXITERR


----<=========================================================================>--
----< Step 9 - Hist version of step 8
----<=========================================================================>--
--!!!RESOLVE EWI!!! /*** SSC-EWI-0013 - EXCEPTION THROWN WHILE CONVERTING ITEM: Mobilize.T12Data.Sql.Ast.TdDelete. LINE: 281 OF FILE: /Users/psundaram/Documents/prjs/cba/agentic/bteq/prtf_tech_own_psst.sql ***/!!!


--Delete
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '281' COLUMN '7' OF THE SOURCE CODE STARTING AT 'from'. EXPECTED 'FROM' GRAMMAR. LAST MATCHING TOKEN WAS 'from' ON LINE '281' COLUMN '7'. FAILED TOKEN WAS '%' ON LINE '281' COLUMN '12'. **
--       from %%STARDATADB%%.DERV_PRTF_OWN_HIST_PSST
;

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.IF ERRORCODE <> 0    THEN .GOTO EXITERR

-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '285' COLUMN '0' OF THE SOURCE CODE STARTING AT 'Insert'. EXPECTED 'Insert' GRAMMAR. LAST MATCHING TOKEN WAS 'into' ON LINE '285' COLUMN '7'. **
--Insert into %%STARDATADB%%.DERV_PRTF_OWN_HIST_PSST
--Select Distinct
--	 DT2.INT_GRUP_I
--	,DT2.ROLE_PLAY_I
--  ,MIN(DT2.PERD_D) Over ( Partition By DT2.INT_GRUP_I, DT2.ROLE_PLAY_I, DT2.GRUP_N ) as JOIN_FROM_D
--  ,MAX(DT2.PERD_D) Over ( Partition By DT2.INT_GRUP_I, DT2.ROLE_PLAY_I, DT2.GRUP_N ) as JOIN_TO_D
--	,DT2.EFFT_D
--	,DT2.EXPY_D
--	,DT2.VALD_FROM_D
--	,DT2.VALD_TO_D
--  ,DT2.ROLE_PLAY_TYPE_X
--	,DT2.DERV_PRTF_ROLE_C
--	,DT2.SRCE_SYST_C
--	,DT2.ROW_SECU_ACCS_C
--	,DT2.PROS_KEY_I
--From
--  (
--		Select Distinct
--			 C.INT_GRUP_I
--			,C.ROLE_PLAY_I
--			,C.EFFT_D
--			,C.EXPY_D
--			,C.VALD_FROM_D
--			,C.VALD_TO_D
--			,C.PERD_D
--			,C.ROLE_PLAY_TYPE_X
--			,C.DERV_PRTF_ROLE_C
--			,C.SRCE_SYST_C
--			,C.ROW_N
--			,C.ROW_SECU_ACCS_C
--			,C.PROS_KEY_I
--      ,MAX(Coalesce( DT1.ROW_N, 0 )) Over (Partition By C.INT_GRUP_I, C.ROLE_PLAY_I, C.PERD_D) as GRUP_N
--		From
--			%%VTECH%%.DERV_PRTF_INT_GRUP_OWN_PSST C
--			Left Join (
--				-- Detect the change in non-key values between rows
--				Select
--           A.INT_GRUP_I
--          ,A.ROLE_PLAY_I
--          ,A.DERV_PRTF_ROLE_C
--          ,A.ROLE_PLAY_TYPE_X
--          ,A.ROW_N
--          ,A.PERD_D
--          ,A.EFFT_D
--				From
--					%%VTECH%%.DERV_PRTF_INT_GRUP_OWN_PSST A
--					Inner Join %%VTECH%%.DERV_PRTF_INT_GRUP_OWN_PSST B

--					On A.INT_GRUP_I = B.INT_GRUP_I
--					And A.ROW_N = B.ROW_N + 1
--					And A.DERV_PRTF_ROLE_C = B.DERV_PRTF_ROLE_C
--          And A.ROLE_PLAY_TYPE_X = B.ROLE_PLAY_TYPE_X

--          -- Detect the change
--          And (
--            A.ROLE_PLAY_TYPE_X <> B.ROLE_PLAY_TYPE_X
--            Or A.ROLE_PLAY_I <> B.ROLE_PLAY_I
--  					Or  A.EFFT_D <> B.EFFT_D
--					)

--			) DT1
--			On C.INT_GRUP_I = DT1.INT_GRUP_I
--			And C.ROLE_PLAY_TYPE_X = DT1.ROLE_PLAY_TYPE_X
--			And C.EFFT_D = DT1.EFFT_D
--      And C.ROW_N >= DT1.ROW_N
--      And DT1.PERD_D <= C.PERD_D

--  ) DT2
;

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.IF ERRORCODE <> 0    THEN .GOTO EXITERR

-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '356' COLUMN '1' OF THE SOURCE CODE STARTING AT 'Collect'. EXPECTED 'STATEMENT' GRAMMAR. LAST MATCHING TOKEN WAS 'Collect' ON LINE '356' COLUMN '1'. **
--Collect Statistics on %%STARDATADB%%.DERV_PRTF_OWN_HIST_PSST
                                                            ;

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.IF ERRORCODE <> 0    THEN .GOTO EXITERR



----<=========================================================================>--
----< Step 10 - Refresh table from Step 8
----<=========================================================================>--
--!!!RESOLVE EWI!!! /*** SSC-EWI-0013 - EXCEPTION THROWN WHILE CONVERTING ITEM: Mobilize.T12Data.Sql.Ast.TdDelete. LINE: 364 OF FILE: /Users/psundaram/Documents/prjs/cba/agentic/bteq/prtf_tech_own_psst.sql ***/!!!
--Delete A
---- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '366' COLUMN '1' OF THE SOURCE CODE STARTING AT 'From'. EXPECTED 'FROM' GRAMMAR. LAST MATCHING TOKEN WAS 'From' ON LINE '366' COLUMN '1'. FAILED TOKEN WAS '%' ON LINE '367' COLUMN '3'. **
--From
--	 %%STARDATADB%%.DERV_PRTF_OWN_PSST A
--	,%%VTECH%%.DERV_PRTF_OWN_HIST_PSST B
--Where
--	A.INT_GRUP_I = B.INT_GRUP_I
--  And A.DERV_PRTF_ROLE_C = B.DERV_PRTF_ROLE_C
--  And PUBLIC.PERIOD_OVERLAPS_UDF(ARRAY_CONSTRUCT( (A.JOIN_FROM_D, A.JOIN_TO_D), (B.JOIN_FROM_D,B.JOIN_TO_D))) !!!RESOLVE EWI!!! /*** SSC-EWI-TD0053 - SNOWFLAKE DOES NOT SUPPORT THE PERIOD DATATYPE, ALL PERIODS ARE HANDLED AS VARCHAR INSTEAD ***/!!!
;

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.IF ERRORCODE <> 0    THEN .GOTO EXITERR

-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '376' COLUMN '0' OF THE SOURCE CODE STARTING AT 'Insert'. EXPECTED 'Insert' GRAMMAR. LAST MATCHING TOKEN WAS 'into' ON LINE '376' COLUMN '7'. **
--Insert into %%STARDATADB%%.DERV_PRTF_OWN_PSST
--Select
--   INT_GRUP_I
--  ,JOIN_FROM_D
--  ,(Case
--      WHEN JOIN_TO_D = ADD_MONTHS(CURRENT_DATE, 1) Then ('9999-12-31'(Date,format'yyyy-mm-dd'))
--      ELSE JOIN_TO_D
--    End
--   ) as JOIN_TO_D
--  ,VALD_FROM_D
--  ,VALD_TO_D
--  ,EFFT_D
--  ,EXPY_D
--  ,DERV_PRTF_ROLE_C
--  ,ROLE_PLAY_TYPE_X
--  ,ROLE_PLAY_I
--  ,SRCE_SYST_C
--  ,ROW_SECU_ACCS_C
--  ,PROS_KEY_I
--From
--  %%VTECH%%.DERV_PRTF_OWN_HIST_PSST
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