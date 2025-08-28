-- =====================================================================
-- DBT Model: prtf_tech_paty_psst
-- Converted from BTEQ: prtf_tech_paty_psst.sql
-- Category: portfolio_technical
-- Original Size: 6.5KB, 228 lines
-- Complexity Score: 73
-- Generated: 2025-08-21 15:46:51
-- =====================================================================

{{ intermediate_model_config() }}



------------------------------------------------------------------------------
--
--  Ver  Date       Modified By        Description
--  ---- ---------- ------------------ ---------------------------------------
--  1.0  18/07/2013 T Jelliffe         Initial Version
--  1.1  27/08/2013 T Jelliffe         Use only records with same EFFT_D
--  1.2  01/11/2013 T Jelliffe         Bug fixes - Row_Number not Rank
--  1.3  27/11/2013 T Jelliffe         Fix overlap calc, JOIN_DATE to calendar
------------------------------------------------------------------------------
-- This info is for CBM use only
-- $LastChangedBy: jelifft $
-- $LastChangedDate: 2013-11-27 14:06:55 +1100 (Wed, 27 Nov 2013) $
-- $LastChangedRevision: 13104 $
--

--<============================================>--
--< STEP 6 - Rule 1 Multiple party to IG       >--
--<============================================>--
-- Original -- Original DELETE removed: DELETE removed: Delete From {{ bteq_var("STARDATADB") }}.DERV_PRTF_PATY_INT_GRUP_PSST
;
-- DBT handles table replacement via materialization strategy
-- DBT handles table replacement via materialization strategy

-- Original INSERT converted to SELECT for DBT intermediate model
Select
	 DT2.INT_GRUP_I                    
	,DT2.PATY_I      
	,DT2.EFFT_D                        
	,DT2.EXPY_D                        
	,DT2.VALD_FROM_D                   
	,DT2.VALD_TO_D     
	,DT2.PERD_D                
	,DT2.REL_C                         
	,DT2.SRCE_SYST_C                   
	,ROW_NUMBER() OVER (Partition By DT2.PATY_I, DT2.REL_C  Order by DT2.PERD_D ) as GRUP_N
	,DT2.ROW_SECU_ACCS_C               
	,DT2.PROS_KEY_I   	
From
	(
		Select
				 DT1.INT_GRUP_I                    
				,DT1.PATY_I      
				,C.CALENDAR_DATE as PERD_D                                    
				,DT1.EFFT_D                        
				,DT1.EXPY_D                        
				,DT1.VALD_FROM_D                   
				,DT1.VALD_TO_D                     
				,DT1.REL_C                         
				,DT1.SRCE_SYST_C                   
				,DT1.ROW_SECU_ACCS_C               
				,DT1.PROS_KEY_I   
		From
			(
				Select
           A.INT_GRUP_I                    
          ,A.PATY_I                        
          ,A.JOIN_FROM_D
          ,A.JOIN_TO_D
          ,A.VALD_FROM_D
          ,A.VALD_TO_D
          ,A.EFFT_D                        
          ,A.EXPY_D                                         
          ,A.REL_C                         
          ,A.SRCE_SYST_C                   
          ,A.ROW_SECU_ACCS_C               
          ,A.PROS_KEY_I                    
				From
					{{ bteq_var("VTECH") }}.DERV_PRTF_PATY_PSST A
					Inner Join {{ bteq_var("VTECH") }}.DERV_PRTF_PATY_PSST B
					On A.PATY_I = B.PATY_I
					and A.REL_C = B.REL_C
--					And A.INT_GRUP_I <> B.INT_GRUP_I
					And (A.JOIN_FROM_D,A.JOIN_TO_D) Overlaps (B.JOIN_FROM_D, B.JOIN_TO_D)
          And (
            A.JOIN_FROM_D <> B.JOIN_FROM_D
            Or A.JOIN_TO_D <> B.JOIN_TO_D

            -- New record
            Or A.INT_GRUP_I <> B.INT_GRUP_I
          )			
					
			) DT1

			Inner Join {{ bteq_var("VTECH") }}.CALENDAR C
			--On C.CALENDAR_DATE between DT1.VALD_FROM_D and DT1.VALD_TO_D
			On C.CALENDAR_DATE between DT1.JOIN_FROM_D and DT1.JOIN_TO_D
			And C.CALENDAR_DATE between DATEADD(MONTH, -39, (Current_Date - EXTRACT(DAY from CURRENT_DATE) +1 )) and DATEADD(MONTH, 1, Current_Date)

		Qualify Row_Number() Over( Partition By DT1.PATY_I, DT1.REL_C, C.CALENDAR_DATE Order BY DT1.EFFT_D Desc, DT1.INT_GRUP_I Desc) = 1

	) DT2;	



--<==========================================>--
--< STEP 7 - Do the history version of above >--
--<==========================================>--

-- Original -- Original DELETE removed: DELETE removed: Delete from {{ bteq_var("STARDATADB") }}.DERV_PRTF_PATY_HIST_PSST All
;
-- DBT handles table replacement via materialization strategy
-- DBT handles table replacement via materialization strategy


-- Original INSERT converted to SELECT for DBT intermediate model
Select Distinct
   DT2.INT_GRUP_I
  ,DT2.PATY_I
  ,DT2.REL_C
  ,MIN(DT2.PERD_D) Over ( Partition By DT2.PATY_I, DT2.INT_GRUP_I, DT2.GRUP_N ) as JOIN_FROM_D
  ,MAX(DT2.PERD_D) Over ( Partition By DT2.PATY_I, DT2.INT_GRUP_I, DT2.GRUP_N ) as JOIN_TO_D
  ,DT2.VALD_FROM_D
  ,DT2.VALD_TO_D
  ,DT2.EFFT_D
  ,DT2.EXPY_D
  ,DT2.SRCE_SYST_C
  ,DT2.ROW_SECU_ACCS_C
  ,DT2.PROS_KEY_I
From
  (
  Select
     C.INT_GRUP_I
    ,C.PATY_I
    ,C.REL_C
    ,C.VALD_FROM_D
    ,C.VALD_TO_D
    ,C.EFFT_D
    ,C.EXPY_D
    ,C.SRCE_SYST_C
    ,C.ROW_SECU_ACCS_C
    ,C.PERD_D
    ,MAX(Coalesce( DT1.ROW_N, 0 )) Over (Partition By C.INT_GRUP_I, C.PATY_I, C.REL_C, C.PERD_D) as GRUP_N 	
    ,C.PROS_KEY_I
  From
    {{ bteq_var("VTECH") }}.DERV_PRTF_PATY_INT_GRUP_PSST C
    Left Join (
      -- Detect the change in non-key values between rows
      Select
         A.INT_GRUP_I
        ,A.PATY_I
        ,A.REL_C
        ,A.ROW_N
        ,A.PERD_D
        ,A.EFFT_D
        ,A.EXPY_D
      From
        {{ bteq_var("VTECH") }}.DERV_PRTF_PATY_INT_GRUP_PSST A
        Inner Join {{ bteq_var("VTECH") }}.DERV_PRTF_PATY_INT_GRUP_PSST B
        On A.PATY_I = B.PATY_I
        And A.REL_C = B.REL_C
        And A.ROW_N = B.ROW_N + 1
        And A.EFFT_D <> B.EFFT_D    
     
    ) DT1
    On C.INT_GRUP_I = DT1.INT_GRUP_I
    And C.PATY_I = DT1.PATY_I
    And C.REL_C = DT1.REL_C
    And C.ROW_N >= DT1.ROW_N
    And DT1.PERD_D <= C.PERD_D 
  ) DT2;


--<================================================>--
--< STEP 8 -- Original DELETE removed: Delete all the original overlap records >--
--<================================================>--

Delete
	A
From 
	 {{ bteq_var("STARDATADB") }}.DERV_PRTF_PATY_PSST A
	,{{ bteq_var("VTECH") }}.DERV_PRTF_PATY_HIST_PSST B
Where  
  A.PATY_I = B.PATY_I
  And A.REL_C = B.REL_C
  And (A.JOIN_FROM_D,A.JOIN_TO_D) overlaps (B.JOIN_FROM_D,B.JOIN_TO_D)   
;
-- DBT handles table replacement via materialization strategy


--<========>--
--< STEP 9 >--
--< Insert the updated records with corrected VALD dates
--<========>--
-- Original INSERT converted to SELECT for DBT intermediate model
Select
   INT_GRUP_I                    
  ,PATY_I                        
  ,JOIN_FROM_D                   
  ,(Case
      WHEN JOIN_TO_D = DATEADD(MONTH, 1, CURRENT_DATE) Then ('9999-12-31'(Date,format'yyyy-mm-dd'))             
      ELSE JOIN_TO_D
    End
    ) as JOIN_TO_D                       
  ,EFFT_D                        
  ,EXPY_D                        
  ,VALD_FROM_D                   
  ,VALD_TO_D                     
  ,REL_C                         
  ,SRCE_SYST_C                   
  ,ROW_SECU_ACCS_C               
  ,PROS_KEY_I                                                 
From 
  {{ bteq_var("VTECH") }}.DERV_PRTF_PATY_HIST_PSST;


Collect Statistics on {{ bteq_var("STARDATADB") }}.DERV_PRTF_PATY_PSST;



