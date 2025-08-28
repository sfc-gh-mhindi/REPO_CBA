.RUN FILE=%%BTEQ_LOGON_SCRIPT%%
.IF ERRORCODE <> 0    THEN .GOTO EXITERR

.SET QUIET OFF
.SET ECHOREQ ON
.SET FORMAT OFF
.SET WIDTH 120

------------------------------------------------------------------------------
--
--  Ver  Date       Modified By        Description
--  ---- ---------- ------------------ ---------------------------------------
--  1.0  18/07/2013 T Jelliffe         Initial Version
--  1.1  27/08/2013 T Jelliffe         Use only records with same EFFT_D
--  1.2  11/10/2013 T Jelliffe         Add GRD join to reduce dataset size
--  1.3  15/10/2013 T Jelliffe         New persist AIG join table and GRD_HIST
--  1.4  30/10/2013 T Jelliffe         Fix ranking for Step 6/7 hist. PERD_D
--  1.5  01/11/2013 T Jelliffe         Bug fixes - Row_Number not Rank
--  1.6  11/11/2013 T Jelliffe         Bug fix - Nathan duplicates
--  1.7  27/11/2013 T Jelliffe         Fix overlap calc, JOIN_DATE to calendar
------------------------------------------------------------------------------
-- This info is for CBM use only
-- $LastChangedBy: jelifft $
-- $LastChangedDate: 2013-11-27 13:59:10 +1100 (Wed, 27 Nov 2013) $
-- $LastChangedRevision: 13103 $
--

      
--<========>--
--< STEP 6 >--
--<========>--
Delete from %%STARDATADB%%.DERV_PRTF_ACCT_INT_GRUP_PSST All
;
.IF ERRORCODE <> 0    THEN .GOTO EXITERR

Insert into %%STARDATADB%%.DERV_PRTF_ACCT_INT_GRUP_PSST
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
	,ROW_NUMBER() OVER (Partition By DT2.ACCT_I, DT2.REL_C Order by DT2.PERD_D ) as GRUP_N
	,DT2.ROW_SECU_ACCS_C
  ,0                          as PROS_KEY_I
From
	(
		Select Distinct
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
					%%VTECH%%.DERV_PRTF_ACCT_PSST A
					Inner Join %%VTECH%%.DERV_PRTF_ACCT_PSST B
					On A.ACCT_I = B.ACCT_I
					and A.REL_C = B.REL_C
          -- Nathan bug fix
					--And A.INT_GRUP_I <> B.INT_GRUP_I
					--And (A.JOIN_FROM_D,A.JOIN_TO_D) Overlaps (B.JOIN_FROM_D, B.JOIN_TO_D)

          And A.JOIN_TO_D >= B.JOIN_FROM_D
          And A.JOIN_FROM_D <= B.JOIN_TO_D

          And (
            --A.JOIN_FROM_D <> B.JOIN_FROM_D
            --Or A.JOIN_TO_D <> B.JOIN_TO_D
            A.EFFT_D <> B.EFFT_D
            Or B.EXPY_D <> B.EXPY_D
            -- Nathan bug fix
            Or A.INT_GRUP_I <> B.INT_GRUP_I
            Or A.PROS_KEY_I <> B.PROS_KEY_I
          )
					
			) DT1

			Inner Join %%VTECH%%.CALENDAR C
			--On C.CALENDAR_DATE between DT1.VALD_FROM_D and DT1.VALD_TO_D
			On C.CALENDAR_DATE between DT1.JOIN_FROM_D and DT1.JOIN_TO_D
			And C.CALENDAR_DATE between ADD_MONTHS((Current_Date - EXTRACT(DAY from CURRENT_DATE) +1 ),-39) and ADD_MONTHS(Current_Date, 1)

    -- Have to also order by INT_GRUP_I in case both have same EFFT_D
		Qualify Row_Number() Over( Partition By DT1.ACCT_I, DT1.REL_C, C.CALENDAR_DATE Order BY DT1.EFFT_D Desc, DT1.INT_GRUP_I Desc) = 1

	) DT2
;	
.IF ERRORCODE <> 0    THEN .GOTO EXITERR


--<========>--
--< STEP 7 >--
--<========>--
Delete from %%STARDATADB%%.DERV_PRTF_ACCT_HIST_PSST All
;
.IF ERRORCODE <> 0    THEN .GOTO EXITERR

Insert into %%STARDATADB%%.DERV_PRTF_ACCT_HIST_PSST
/* Working History for ACCT data */
Select      
   DT3.INT_GRUP_I                    
  ,DT3.ACCT_I                        
  ,DT3.REL_C                         
  ,DT3.JOIN_FROM_D                   
  ,DT3.JOIN_TO_D                     
  ,DT3.VALD_FROM_D                   
  ,DT3.VALD_TO_D                     
  ,DT3.EFFT_D                        
  ,DT3.EXPY_D                        
  ,DT3.SRCE_SYST_C                   
  ,DT3.ROW_SECU_ACCS_C              
From
	(
    Select
       DT2.INT_GRUP_I
      ,DT2.ACCT_I
      ,DT2.REL_C
      ,DT2.EFFT_D
      ,DT2.EXPY_D
      ,DT2.VALD_FROM_D
      ,DT2.VALD_TO_D
      ,DT2.PERD_D
      ,MIN(DT2.PERD_D) Over (Partition By DT2.ACCT_I, DT2.REL_C, DT2.GRUP_N) as JOIN_FROM_D
      ,MAX(DT2.PERD_D) Over (Partition by DT2.ACCT_I, DT2.REL_C, DT2.GRUP_N )  as JOIN_TO_D
      ,DT2.SRCE_SYST_C
      ,DT2.GRUP_N
      ,DT2.ROW_SECU_ACCS_C
    From
      (
        Select
           PAIG.INT_GRUP_I
          ,PAIG.ACCT_I
          ,PAIG.REL_C
          ,PAIG.PERD_D
          ,PAIG.EFFT_D
          ,PAIG.EXPY_D
          ,PAIG.VALD_FROM_D
          ,PAIG.VALD_TO_D
          ,PAIG.SRCE_SYST_C
          ,MAX(Coalesce( DT1.ROW_N, 0 )) Over (Partition By PAIG.ACCT_I, PAIG.INT_GRUP_I, PAIG.PERD_D) as GRUP_N        
          ,PAIG.ROW_SECU_ACCS_C
         From
          %%VTECH%%.DERV_PRTF_ACCT_INT_GRUP_PSST PAIG
          Left Join (
            Select
               A.INT_GRUP_I
              ,A.ACCT_I
              ,A.REL_C
              ,A.ROW_N
              ,A.PERD_D
              ,A.EFFT_D
              ,A.EXPY_D
            From
              %%VTECH%%.DERV_PRTF_ACCT_INT_GRUP_PSST A
              Inner Join %%VTECH%%.DERV_PRTF_ACCT_INT_GRUP_PSST B
              On A.ACCT_I = B.ACCT_I
              And A.REL_C = B.REL_C
              And A.ROW_N = B.ROW_N + 1
              And (
                A.EFFT_D <> B.EFFT_D
                Or A.INT_GRUP_I <> B.INT_GRUP_I
              )
          ) DT1
          On DT1.INT_GRUP_I = PAIG.INT_GRUP_I
          And DT1.ACCT_I = PAIG.ACCT_I
          And DT1.REL_C = PAIG.REL_C
          And DT1.EFFT_D = PAIG.EFFT_D
          And DT1.EXPY_D = PAIG.EXPY_D
          And PAIG.ROW_N >= DT1.ROW_N
          And DT1.PERD_D <= PAIG.PERD_D
      ) DT2

  ) DT3
Group By 1,2,3,4,5,6,7,8,9,10,11
;
.IF ERRORCODE <> 0    THEN .GOTO EXITERR

Collect Statistics on %%STARDATADB%%.DERV_PRTF_ACCT_HIST_PSST
;
.IF ERRORCODE <> 0    THEN .GOTO EXITERR


--<========>--
--< STEP 8 >--
--< Remove the records with the old VALD dates
--<========>--

--< To handle case when some records blanked out completely use the original 
--< Overlap query to determine which PK values get deleted.


--< Check the delete actually removes all relevant records!

Delete
  A
From
	%%STARDATADB%%.DERV_PRTF_ACCT_PSST A
  ,%%VTECH%%.DERV_PRTF_ACCT_HIST_PSST B
Where
  A.ACCT_I = B.ACCT_I
  And A.REL_C = B.REL_C
  --And (A.JOIN_FROM_D,A.JOIN_TO_D) Overlaps (B.JOIN_FROM_D, B.JOIN_TO_D)
  And A.JOIN_TO_D >= B.JOIN_FROM_D
  And A.JOIN_FROM_D <= B.JOIN_TO_D 
;
.IF ERRORCODE <> 0    THEN .GOTO EXITERR


--<========>--
--< STEP 9 >--
--< Insert the updated records with corrected VALD dates
--<========>--
Insert into %%STARDATADB%%.DERV_PRTF_ACCT_PSST
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
  %%VTECH%%.DERV_PRTF_ACCT_HIST_PSST
;
.IF ERRORCODE <> 0    THEN .GOTO EXITERR


-- Check this by joining the current prod persist table to GRD and counting the results!


.QUIT 0
.LOGOFF

.LABEL EXITERR
.QUIT 1
.LOGOFF
.EXIT 