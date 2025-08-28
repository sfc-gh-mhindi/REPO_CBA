.RUN FILE=${ETL_APP_BTEQ}/bteq_login.sql
.IF ERRORCODE <> 0    THEN .GOTO EXITERR

.SET QUIET OFF
.SET ECHOREQ ON
.SET FORMAT OFF
.SET WIDTH 120

------------------------------------------------------------------------------
--
--  Ver  Date       Modified By        Description
--  ---- ---------- ------------------ ---------------------------------------
--  1.0  11/06/2013 T Jelliffe         Initial Version
--  1.1  11/07/2013 T Jelliffe         Use PROS_KEY_EFFT_I prevent self join
--  1.2  12/07/2013 T Jelliffe         Time period reduced 15 to 3 years
--  1.3  31/07/2013 Z Lwin             Remove EROR_SEQN_I
--  1.4  27/08/2013 T Jelliffe         Use only records with same EFFT_D
--  1.5  15/10/2013 T Jelliffe         All overlap records recalculated
--  1.6  01/11/2013 T Jelliffe         Bug fixes - Row_Number not Rank
--  1.7  27/11/2013 T Jelliffe         Fix overlap calc, JOIN_DATE to calendar
------------------------------------------------------------------------------
-- This info is for CBM use only
-- $LastChangedBy: jelifft $
-- $LastChangedDate: 2013-11-27 13:59:10 +1100 (Wed, 27 Nov 2013) $
-- $LastChangedRevision: 13103 $

--<============================================>--
--< STEP 1 - AIG group                         >--
--<============================================>--
Delete from STAR_CAD_PROD_DATA.DERV_PRTF_ACCT_PSST
;
.IF ERRORCODE <> 0    THEN .GOTO EXITERR

Insert into STAR_CAD_PROD_DATA.DERV_PRTF_ACCT_PSST
Select
   AIG.INT_GRUP_I               as INT_GRUP_I
  ,AIG.ACCT_I                   as ACCT_I
  ,AIG.REL_C
  ,AIG.VALD_FROM_D              as JOIN_FROM_D
  ,AIG.VALD_TO_D			          as JOIN_TO_D
  ,AIG.VALD_FROM_D
  ,AIG.VALD_TO_D
  ,AIG.EFFT_D
  ,AIG.EXPY_D
  ,AIG.SRCE_SYST_C
  ,AIG.PROS_KEY_EFFT_I
  ,AIG.ROW_SECU_ACCS_C
From
  PVTECH.ACCT_INT_GRUP AIG

  /* Add the GRD filter to reduce the data */
  INNER JOIN PVTECH.INT_GRUP IG
  ON IG.INT_GRUP_I = AIG.INT_GRUP_I
    
  INNER JOIN PVTECH.GRD_PRTF_TYPE_ENHC_HIST_PSST GPTE  
  ON GPTE.PRTF_TYPE_C = IG.INT_GRUP_TYPE_C

  -- Overlaps
  --AND (GPTE.VALD_FROM_D,GPTE.VALD_TO_D) Overlaps (IG.CRAT_D,IG.VALD_TO_D)
  And GPTE.VALD_TO_D >= IG.CRAT_D
  And GPTE.VALD_FROM_D <= IG.VALD_TO_D

  --AND (GPTE.VALD_FROM_D,GPTE.VALD_TO_D) Overlaps (AIG.VALD_FROM_D,AIG.VALD_TO_D)
  And GPTE.VALD_TO_D >= AIG.VALD_FROM_D
  And GPTE.VALD_FROM_D <= AIG.VALD_TO_D
   
Group By 1,2,3,4,5,6,7,8,9,10,11,12
; 
.IF ERRORCODE <> 0    THEN .GOTO EXITERR


Collect statistics on STAR_CAD_PROD_DATA.DERV_PRTF_ACCT_PSST
;
.IF ERRORCODE <> 0    THEN .GOTO EXITERR


--<============================================>--
--< STEP 2 - Overlapping Business period dates >--
--<============================================>--
Delete from STAR_CAD_PROD_DATA.DERV_PRTF_ACCT_INT_GRUP_PSST All
;
.IF ERRORCODE <> 0    THEN .GOTO EXITERR

Insert into STAR_CAD_PROD_DATA.DERV_PRTF_ACCT_INT_GRUP_PSST
Select
	 DT2.INT_GRUP_I       
	,DT2.ACCT_I
	,DT2.EFFT_D
	,DT2.EXPY_D
	,DT2.VALD_FROM_D
	,DT2.VALD_TO_D
	,DT2.PERD_D
	,DT2.REL_C
	,DT2.SRCE_SYST_C
	,ROW_NUMBER() OVER (Partition By DT2.INT_GRUP_I, DT2.ACCT_I Order by DT2.PERD_D, DT2.REL_C ) as GRUP_N
	,DT2.ROW_SECU_ACCS_C
  ,0                          as PROS_KEY_I
From
	(
		Select
			 DT1.INT_GRUP_I       
			,DT1.ACCT_I
			,DT1.EFFT_D
			,DT1.EXPY_D
			,DT1.VALD_FROM_D
			,DT1.VALD_TO_D
			,C.CALENDAR_DATE as PERD_D
			,DT1.REL_C
			,DT1.SRCE_SYST_C
			,DT1.ROW_SECU_ACCS_C
		From
			(
				Select
					 A.INT_GRUP_I
					,A.ACCT_I
					,A.REL_C
					,A.EFFT_D
					,A.EXPY_D
					,A.VALD_FROM_D
					,A.VALD_TO_D
          ,A.JOIN_FROM_D
          ,A.JOIN_TO_D
					,A.SRCE_SYST_C
					,A.ROW_SECU_ACCS_C
				From
          PVTECH.DERV_PRTF_ACCT_PSST A
          Inner Join PVTECH.DERV_PRTF_ACCT_PSST B
					On A.ACCT_I = B.ACCT_I
					And A.INT_GRUP_I = B.INT_GRUP_I
					--And (A.JOIN_FROM_D,A.JOIN_TO_D) overlaps (B.JOIN_FROM_D,B.JOIN_TO_D)
          And A.JOIN_TO_D >= B.JOIN_FROM_D
          And A.JOIN_FROM_D <= B.JOIN_TO_D

          And (
            --A.JOIN_FROM_D <> B.JOIN_FROM_D
            --Or A.JOIN_TO_D <> B.JOIN_TO_D
            -- New to fix extra Nathan
            A.EFFT_D <> B.EFFT_D
            Or A.EXPY_D <> B.EXPY_D
            Or A.PROS_KEY_I <> B.PROS_KEY_I
          )
 				
			) DT1

			Inner Join PVTECH.CALENDAR C
			--On C.CALENDAR_DATE between DT1.VALD_FROM_D and DT1.VALD_TO_D
			On C.CALENDAR_DATE between DT1.JOIN_FROM_D and DT1.JOIN_TO_D
			And C.CALENDAR_DATE between ADD_MONTHS((Current_Date - EXTRACT(DAY from CURRENT_DATE) +1 ),-39) and ADD_MONTHS(Current_Date, 1)

		Qualify Row_Number() Over( Partition By DT1.ACCT_I, DT1.INT_GRUP_I, C.CALENDAR_DATE Order BY DT1.EFFT_D Desc, DT1.REL_C Desc) = 1

	) DT2
;	
.IF ERRORCODE <> 0    THEN .GOTO EXITERR

Collect Statistics on STAR_CAD_PROD_DATA.DERV_PRTF_ACCT_INT_GRUP_PSST
;
.IF ERRORCODE <> 0    THEN .GOTO EXITERR


--<============================================>--
--< STEP 3 - Calculate correct history         >--
--<============================================>--
Delete from STAR_CAD_PROD_DATA.DERV_PRTF_ACCT_HIST_PSST All
;
.IF ERRORCODE <> 0    THEN .GOTO EXITERR


Insert into STAR_CAD_PROD_DATA.DERV_PRTF_ACCT_HIST_PSST
Select Distinct
   DT2.INT_GRUP_I
  ,DT2.ACCT_I
  ,DT2.REL_C
  ,MIN(DT2.PERD_D) Over ( Partition By DT2.ACCT_I, DT2.INT_GRUP_I, DT2.GRUP_N ) as JOIN_FROM_D
  ,MAX(DT2.PERD_D) Over ( Partition By DT2.ACCT_I, DT2.INT_GRUP_I, DT2.GRUP_N ) as JOIN_TO_D
  ,DT2.VALD_FROM_D
  ,DT2.VALD_TO_D
  ,DT2.EFFT_D
  ,DT2.EXPY_D
  ,DT2.SRCE_SYST_C
  ,DT2.ROW_SECU_ACCS_C
From
  (
  Select Distinct
     C.INT_GRUP_I
    ,C.ACCT_I
    ,C.REL_C
    ,C.VALD_FROM_D
    ,C.VALD_TO_D
    ,C.EFFT_D
    ,C.EXPY_D
    ,C.SRCE_SYST_C
    ,C.ROW_SECU_ACCS_C
    ,C.PERD_D
    ,MAX(Coalesce( DT1.ROW_N, 0 )) Over (Partition By C.ACCT_I, C.INT_GRUP_I, C.PERD_D) as GRUP_N     
  From
    PVTECH.DERV_PRTF_ACCT_INT_GRUP_PSST C
    Left Join (
      -- Detect the change in non-key values between rows
      Select
         A.INT_GRUP_I
        ,A.ACCT_I
        ,A.PERD_D
        ,A.REL_C
        ,A.ROW_N
        ,A.EFFT_D
      From
        PVTECH.DERV_PRTF_ACCT_INT_GRUP_PSST A
        Inner Join PVTECH.DERV_PRTF_ACCT_INT_GRUP_PSST B
        On A.ACCT_I = B.ACCT_I
        And A.INT_GRUP_I = B.INT_GRUP_I
        And A.ROW_N = B.ROW_N + 1
        And A.EFFT_D <> B.EFFT_D
    ) DT1
    On C.INT_GRUP_I = DT1.INT_GRUP_I
    And C.ACCT_I = DT1.ACCT_I
    And C.REL_C = DT1.REL_C
    And C.EFFT_D = DT1.EFFT_D
    And C.ROW_N >= DT1.ROW_N
    And DT1.PERD_D <= C.PERD_D    

  ) DT2
;
.IF ERRORCODE <> 0    THEN .GOTO EXITERR

Collect Statistics on STAR_CAD_PROD_DATA.DERV_PRTF_ACCT_HIST_PSST
;
.IF ERRORCODE <> 0    THEN .GOTO EXITERR



--<================================================>--
--< STEP 4 Delete all the original overlap records >--
--<================================================>--

Delete
	A
From 
	 STAR_CAD_PROD_DATA.DERV_PRTF_ACCT_PSST A
	,PVTECH.DERV_PRTF_ACCT_HIST_PSST B
Where  
  A.ACCT_I = B.ACCT_I
  And A.INT_GRUP_I = B.INT_GRUP_I
  --And (A.JOIN_FROM_D,A.JOIN_TO_D) overlaps (B.JOIN_FROM_D,B.JOIN_TO_D)   
  And A.JOIN_TO_D >= B.JOIN_FROM_D
  And A.JOIN_FROM_D <= B.JOIN_TO_D  
;
.IF ERRORCODE <> 0    THEN .GOTO EXITERR


--<================================================>--
--< STEP 5 - Replace with updated records          >--
--<================================================>--

Insert into STAR_CAD_PROD_DATA.DERV_PRTF_ACCT_PSST
Select
   INT_GRUP_I                    
  ,ACCT_I                        
  ,REL_C                         
  ,JOIN_FROM_D                
  ,(Case
      WHEN JOIN_TO_D = ADD_MONTHS(CURRENT_DATE, 1) Then ('9999-12-31'(Date,format'yyyy-mm-dd'))             
      ELSE JOIN_TO_D
    End
    ) as JOIN_TO_D
  ,VALD_FROM_D
  ,VALD_TO_D
  ,EFFT_D                        
  ,EXPY_D                        
  ,SRCE_SYST_C 
  ,0 as PROS_KEY_EFFT_I  
  ,ROW_SECU_ACCS_C               
From 
  PVTECH.DERV_PRTF_ACCT_HIST_PSST
;
.IF ERRORCODE <> 0    THEN .GOTO EXITERR


.QUIT 0
.LOGOFF

.LABEL EXITERR
.QUIT 1
.LOGOFF
.EXIT 
 
