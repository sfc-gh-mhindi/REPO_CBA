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
----  1.5  01/11/2013 T Jelliffe         Bug fixes - Row_Number not Rank
----  1.6  08/11/2013 T Jelliffe         Filter first on PTCL_N integer only
--------------------------------------------------------------------------------
----
---- This info is for CBM use only
---- $LastChangedBy: jelifft $
---- $LastChangedDate: 2013-11-08 14:22:54 +1100 (Fri, 08 Nov 2013) $
---- $LastChangedRevision: 12989 $
----


----<=============================================>--
----< STEP 1 - Keep a copy of INT_GRUP base table >--
----<=============================================>--
--!!!RESOLVE EWI!!! /*** SSC-EWI-0013 - EXCEPTION THROWN WHILE CONVERTING ITEM: Mobilize.T12Data.Sql.Ast.TdDelete. LINE: 33 OF FILE: /Users/mhindi/Library/CloudStorage/GoogleDrive-mazen.hindi@snowflake.com/My Drive/Work/Code Repos/CBA/REPO_CBA/GDW1/BTEQ/prtf_tech_int_grup_enhc_psst.sql ***/!!!

--Delete
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '33' COLUMN '7' OF THE SOURCE CODE STARTING AT 'from'. EXPECTED 'FROM' GRAMMAR. LAST MATCHING TOKEN WAS 'from' ON LINE '33' COLUMN '7'. FAILED TOKEN WAS '%' ON LINE '33' COLUMN '12'. **
--       from %%STARDATADB%%.DERV_PRTF_INT_PSST
;

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.IF ERRORCODE <> 0    THEN .GOTO EXITERR


-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '38' COLUMN '0' OF THE SOURCE CODE STARTING AT 'Insert'. EXPECTED 'Insert' GRAMMAR. LAST MATCHING TOKEN WAS 'into' ON LINE '38' COLUMN '7'. **
--Insert into %%STARDATADB%%.DERV_PRTF_INT_PSST
--Select
--   A.INT_GRUP_I
--  ,A.INT_GRUP_TYPE_C
--  ,A.CRAT_D as JOIN_FROM_D
--  ,A.VALD_TO_D as  JOIN_TO_D
--  ,A.EFFT_D
--  ,A.EXPY_D
--  ,A.PTCL_N
--  ,A.REL_MNGE_I
--  ,A.CRAT_D as VALD_FROM_D
--  ,A.VALD_TO_D
--  ,A.PROS_KEY_EFFT_I
--From
--	%%VTECH%%.INT_GRUP A

--	/* Use new History table */
--	Inner Join %%VTECH%%.GRD_PRTF_TYPE_ENHC_HIST_PSST GPTE
--	On GPTE.PRTF_TYPE_C = A.INT_GRUP_TYPE_C
--	And (GPTE.VALD_FROM_D, GPTE.VALD_TO_D) Overlaps (A.CRAT_D, A.VALD_TO_D)

--  And CHAR2HEXINT( UPPER(Coalesce(A.PTCL_N,'0') )) = CHAR2HEXINT( LOWER(Coalesce(A.PTCL_N,'0') ))

--Group By 1,2,3,4,5,6,7,8,9,10,11
;

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.IF ERRORCODE <> 0    THEN .GOTO EXITERR

-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '65' COLUMN '1' OF THE SOURCE CODE STARTING AT 'Collect'. EXPECTED 'STATEMENT' GRAMMAR. LAST MATCHING TOKEN WAS 'Collect' ON LINE '65' COLUMN '1'. **
--Collect statistics on %%STARDATADB%%.DERV_PRTF_INT_PSST
;

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.IF ERRORCODE <> 0    THEN .GOTO EXITERR



----<============================================>--
----< STEP 2 - INT_GRUP                          >--
----<============================================>--
--!!!RESOLVE EWI!!! /*** SSC-EWI-0013 - EXCEPTION THROWN WHILE CONVERTING ITEM: Mobilize.T12Data.Sql.Ast.TdDelete. LINE: 74 OF FILE: /Users/mhindi/Library/CloudStorage/GoogleDrive-mazen.hindi@snowflake.com/My Drive/Work/Code Repos/CBA/REPO_CBA/GDW1/BTEQ/prtf_tech_int_grup_enhc_psst.sql ***/!!!
--Delete
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '74' COLUMN '7' OF THE SOURCE CODE STARTING AT 'from'. EXPECTED 'FROM' GRAMMAR. LAST MATCHING TOKEN WAS 'from' ON LINE '74' COLUMN '7'. FAILED TOKEN WAS '%' ON LINE '74' COLUMN '12'. **
--       from %%STARDATADB%%.DERV_PRTF_INT_GRUP_ENHC_PSST All
;

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.IF ERRORCODE <> 0    THEN .GOTO EXITERR

-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '78' COLUMN '1' OF THE SOURCE CODE STARTING AT 'Collect'. EXPECTED 'STATEMENT' GRAMMAR. LAST MATCHING TOKEN WAS 'Collect' ON LINE '78' COLUMN '1'. **
--Collect Statistics on %%STARDATADB%%.DERV_PRTF_INT_GRUP_ENHC_PSST
                                                                 ;

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.IF ERRORCODE <> 0    THEN .GOTO EXITERR


-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '82' COLUMN '0' OF THE SOURCE CODE STARTING AT 'Insert'. EXPECTED 'Insert' GRAMMAR. LAST MATCHING TOKEN WAS 'into' ON LINE '82' COLUMN '7'. **
--Insert into %%STARDATADB%%.DERV_PRTF_INT_GRUP_ENHC_PSST
--Select
--   DT2.INT_GRUP_I
--  ,DT2.INT_GRUP_TYPE_C
--  ,DT2.EFFT_D
--  ,DT2.EXPY_D
--  ,DT2.VALD_FROM_D
--  ,DT2.VALD_TO_D
--  ,DT2.PERD_D
--  ,DT2.PTCL_N
--  ,DT2.REL_MNGE_I
--  ,ROW_NUMBER() OVER (Partition By DT2.INT_GRUP_I Order by DT2.PERD_D )
--  ,0 as PROS_KEY
--From
--	(
--				Select
--					 A.INT_GRUP_I
--					,A.INT_GRUP_TYPE_C
--					,A.PTCL_N
--					,C.CALENDAR_DATE as PERD_D
--					,A.EFFT_D
--					,A.EXPY_D
--					,A.VALD_FROM_D
--					,A.VALD_TO_D
--					,A.REL_MNGE_I
--				From
--          %%VTECH%%.DERV_PRTF_INT_PSST A
--          Inner Join %%VTECH%%.DERV_PRTF_INT_PSST B
--					On A.INT_GRUP_I = B.INT_GRUP_I
--					And CHAR2HEXINT( UPPER(Coalesce(A.PTCL_N,'0') )) = CHAR2HEXINT( LOWER(Coalesce(A.PTCL_N,'0') ))
--					And CHAR2HEXINT( UPPER(Coalesce(B.PTCL_N,'0') )) = CHAR2HEXINT( LOWER(Coalesce(B.PTCL_N,'0') ))
--					And (A.JOIN_FROM_D,A.JOIN_TO_D) Overlaps (B.JOIN_FROM_D,B.JOIN_TO_D)
--          And (         -- Updated Wed 6/11
--            A.JOIN_FROM_D <> B.JOIN_FROM_D
--            Or A.JOIN_TO_D <> B.JOIN_TO_D
--            Or A.EFFT_D <> B.EFFT_D
--            Or A.EXPY_D <> B.EXPY_D
--            Or A.PTCL_N <> B.PTCL_N
--            Or A.INT_GRUP_TYPE_C <> B.INT_GRUP_TYPE_C
--            Or A.REL_MNGE_I <> B.REL_MNGE_I
--          )

--					Inner Join %%VTECH%%.CALENDAR C
--					On C.CALENDAR_DATE between A.JOIN_FROM_D and A.JOIN_TO_D
--					And C.CALENDAR_DATE between ADD_MONTHS((Current_Date - EXTRACT(DAY from CURRENT_DATE) +1 ),-39) and ADD_MONTHS(Current_Date, 1)

--		Qualify Row_Number() Over( Partition By A.INT_GRUP_I, C.CALENDAR_DATE Order BY A.EFFT_D Desc) = 1
--	) DT2
;

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.IF ERRORCODE <> 0    THEN .GOTO EXITERR

-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '133' COLUMN '1' OF THE SOURCE CODE STARTING AT 'Collect'. EXPECTED 'STATEMENT' GRAMMAR. LAST MATCHING TOKEN WAS 'Collect' ON LINE '133' COLUMN '1'. **
--Collect Statistics on %%STARDATADB%%.DERV_PRTF_INT_GRUP_ENHC_PSST
                                                                 ;

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.IF ERRORCODE <> 0    THEN .GOTO EXITERR

----<============================================>--
----< STEP 3 - History                           >--
----<============================================>--
--!!!RESOLVE EWI!!! /*** SSC-EWI-0013 - EXCEPTION THROWN WHILE CONVERTING ITEM: Mobilize.T12Data.Sql.Ast.TdDelete. LINE: 139 OF FILE: /Users/mhindi/Library/CloudStorage/GoogleDrive-mazen.hindi@snowflake.com/My Drive/Work/Code Repos/CBA/REPO_CBA/GDW1/BTEQ/prtf_tech_int_grup_enhc_psst.sql ***/!!!
--Delete
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '139' COLUMN '7' OF THE SOURCE CODE STARTING AT 'from'. EXPECTED 'FROM' GRAMMAR. LAST MATCHING TOKEN WAS 'from' ON LINE '139' COLUMN '7'. FAILED TOKEN WAS '%' ON LINE '139' COLUMN '12'. **
--       from %%STARDATADB%%.DERV_PRTF_INT_HIST_PSST All
;

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.IF ERRORCODE <> 0    THEN .GOTO EXITERR

-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '143' COLUMN '0' OF THE SOURCE CODE STARTING AT 'Insert'. EXPECTED 'Insert' GRAMMAR. LAST MATCHING TOKEN WAS 'into' ON LINE '143' COLUMN '7'. **
--Insert into %%STARDATADB%%.DERV_PRTF_INT_HIST_PSST
--Select Distinct
--	 DT2.INT_GRUP_I
--	,DT2.INT_GRUP_TYPE_C
--	,DT2.EFFT_D
--	,DT2.EXPY_D
--	,DT2.VALD_FROM_D
--	,DT2.VALD_TO_D
--  ,MIN(DT2.PERD_D) Over ( Partition By DT2.INT_GRUP_I, DT2.GRUP_N ) as JOIN_FROM_D
--  ,MAX(DT2.PERD_D) Over ( Partition By DT2.INT_GRUP_I, DT2.GRUP_N ) as JOIN_TO_D
--	,DT2.PTCL_N
--	,DT2.REL_MNGE_I
--From
--  (
--  Select
--		 C.INT_GRUP_I
--		,C.INT_GRUP_TYPE_C
--		,C.EFFT_D
--		,C.EXPY_D
--		,C.VALD_FROM_D
--		,C.VALD_TO_D
--		,C.PERD_D
--		,C.PTCL_N
--		,C.REL_MNGE_I
--    ,MAX(Coalesce( DT1.ROW_N, 0 )) Over (Partition By C.INT_GRUP_I, C.PTCL_N, C.REL_MNGE_I, C.PERD_D) as GRUP_N
--  From
--    %%VTECH%%.DERV_PRTF_INT_GRUP_ENHC_PSST C
--    Left Join (
--      -- Detect the change in non-key values between rows
--      Select
--         A.INT_GRUP_I
--        ,A.PTCL_N
--        ,A.REL_MNGE_I
--        ,A.PERD_D
--        ,A.ROW_N
--      From
--        %%VTECH%%.DERV_PRTF_INT_GRUP_ENHC_PSST A
--        Inner Join %%VTECH%%.DERV_PRTF_INT_GRUP_ENHC_PSST B
--        On A.INT_GRUP_I = B.INT_GRUP_I
--        And A.ROW_N = B.ROW_N + 1
--        And (
--					A.INT_GRUP_TYPE_C <> B.INT_GRUP_TYPE_C
--					Or A.PTCL_N <> B.PTCL_N
--					Or A.REL_MNGE_I <> B.REL_MNGE_I
--				)
--    ) DT1
--    On C.INT_GRUP_I = DT1.INT_GRUP_I
--		And C.PTCL_N = DT1.PTCL_N
--		And C.REL_MNGE_I = DT1.REL_MNGE_I
--    And C.ROW_N >= DT1.ROW_N
--    And DT1.PERD_D <= C.PERD_D

--  ) DT2
;

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.IF ERRORCODE <> 0    THEN .GOTO EXITERR


-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '200' COLUMN '1' OF THE SOURCE CODE STARTING AT 'Collect'. EXPECTED 'STATEMENT' GRAMMAR. LAST MATCHING TOKEN WAS 'Collect' ON LINE '200' COLUMN '1'. **
--Collect Statistics on %%STARDATADB%%.DERV_PRTF_INT_HIST_PSST
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