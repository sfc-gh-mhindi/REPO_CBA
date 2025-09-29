-- =====================================================================
-- DBT Model: prtf_tech_paty_int_grup_psst
-- Converted from BTEQ: prtf_tech_paty_int_grup_psst.sql
-- Category: portfolio_technical
-- Original Size: 7.3KB, 252 lines
-- Complexity Score: 99
-- Generated: 2025-08-21 15:55:19
-- =====================================================================

{{ intermediate_model_config() }}



------------------------------------------------------------------------------
--
--  Ver  Date       Modified By        Description
--  ---- ---------- ------------------ ---------------------------------------
--  1.0  11/06/2013 T Jelliffe         Initial Version
--  1.1  11/07/2013 T Jelliffe         Use PROS_KEY_EFFT_I prevent self join
--  1.2  12/07/2013 T Jelliffe         Time period reduced 15 to 3 years
--  1.3  31/07/2013 Z Lwin             Remove EROR_SEQN_I
--  1.4  27/08/2013 T Jelliffe         Use only records with same EFFT_D
--  1.5  21/10/2013 T Jelliffe         Use Insert/-- Original DELETE removed: Delete process
--  1.6  01/11/2013 T Jelliffe         Bug fixes - Row_Number not Rank
------------------------------------------------------------------------------
-- This info is for CBM use only
-- $LastChangedBy: jelifft $
-- $LastChangedDate: 2013-11-06 09:17:02 +1100 (Wed, 06 Nov 2013) $
-- $LastChangedRevision: 12972 $
--

--<============================================>--
--< STEP 1 - PIG group                         >--
--<============================================>--

-- Original DELETE removed: Delete from {{ bteq_var("STARDATADB") }}.DERV_PRTF_PATY_PSST 
;
-- DBT handles table replacement via materialization strategy
-- DBT handles table replacement via materialization strategy

-- Original INSERT converted to SELECT for DBT intermediate model
Select
   PIG.INT_GRUP_I                    
  ,PIG.PATY_I                        
  ,PIG.VALD_FROM_D                   as JOIN_FROM_D
  ,PIG.VALD_TO_D				             as JOIN_TO_D  
  ,PIG.EFFT_D                        
  ,PIG.EXPY_D                        
  ,PIG.VALD_FROM_D                   
  ,PIG.VALD_TO_D                     
  ,PIG.REL_C                         
  ,PIG.SRCE_SYST_C                   
  ,PIG.ROW_SECU_ACCS_C
  ,PIG.PROS_KEY_EFFT_I                as PROS_KEY_I
From
  {{ bteq_var("VTECH") }}.PATY_INT_GRUP PIG

  Inner Join {{ bteq_var("VTECH") }}.INT_GRUP IG
  On PIG.INT_GRUP_I = IG.INT_GRUP_I
  And (PIG.VALD_FROM_D,PIG.VALD_TO_D) Overlaps (IG.CRAT_D, IG.VALD_TO_D)

  INNER JOIN {{ bteq_var("VTECH") }}.GRD_PRTF_TYPE_ENHC_HIST_PSST GPTE  
  ON GPTE.PRTF_TYPE_C = IG.INT_GRUP_TYPE_C
  AND (GPTE.VALD_FROM_D,GPTE.VALD_TO_D) Overlaps (IG.CRAT_D,IG.VALD_TO_D)
  AND (GPTE.VALD_FROM_D,GPTE.VALD_TO_D) Overlaps (PIG.VALD_FROM_D,PIG.VALD_TO_D)
Group By 1,2,3,4,5,6,7,8,9,10,11,12;

Collect statistics on {{ bteq_var("STARDATADB") }}.DERV_PRTF_PATY_PSST
;

--<============================================>--
--< STEP 2 - Daily PIG overlaps                >--
--<============================================>--

-- Original -- Original DELETE removed: DELETE removed: Delete from {{ bteq_var("STARDATADB") }}.DERV_PRTF_PATY_INT_GRUP_PSST All
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
	,ROW_NUMBER() OVER (Partition By DT2.INT_GRUP_I, DT2.PATY_I Order by DT2.PERD_D, DT2.REL_C ) as GRUP_N  
  ,DT2.ROW_SECU_ACCS_C
  ,DT2.PROS_KEY_I
From 
  (
    Select
       A.INT_GRUP_I
      ,A.PATY_I
      ,C.CALENDAR_DATE				as PERD_D             
      ,A.EFFT_D
      ,A.EXPY_D
      ,A.VALD_FROM_D
      ,A.VALD_TO_D
      ,A.REL_C
      ,A.SRCE_SYST_C
      ,A.ROW_SECU_ACCS_C
      ,A.PROS_KEY_I
    From
      {{ bteq_var("VTECH") }}.DERV_PRTF_PATY_PSST A
      Inner Join {{ bteq_var("VTECH") }}.DERV_PRTF_PATY_PSST B
      On A.PATY_I = B.PATY_I
      And A.INT_GRUP_I = B.INT_GRUP_I
      And (A.JOIN_FROM_D,A.JOIN_TO_D) overlaps (B.JOIN_FROM_D,B.JOIN_TO_D)  
      And (
        A.JOIN_FROM_D <> B.JOIN_FROM_D
        Or A.JOIN_TO_D <> B.JOIN_TO_D
      )

      Inner Join {{ bteq_var("VTECH") }}.CALENDAR C
      On C.CALENDAR_DATE between A.VALD_FROM_D and A.VALD_TO_D
      And C.CALENDAR_DATE between DATEADD(MONTH, -39, (Current_Date - EXTRACT(DAY from CURRENT_DATE) +1 )) and DATEADD(MONTH, 1, Current_Date)     

    Qualify Row_Number() Over( Partition By A.PATY_I, A.INT_GRUP_I, C.CALENDAR_DATE Order BY A.EFFT_D Desc) = 1
    Group By 1,2,3,4,5,6,7,8,9,10,11    
  ) DT2;

Collect Statistics on {{ bteq_var("STARDATADB") }}.DERV_PRTF_PATY_INT_GRUP_PSST;


--<============================================>--
--< STEP 3 - History group                     >--
--<============================================>--
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
        ,A.EFFT_D
        ,A.REL_C
        ,A.ROW_N
        ,A.PERD_D
      From
        {{ bteq_var("VTECH") }}.DERV_PRTF_PATY_INT_GRUP_PSST A
        Inner Join {{ bteq_var("VTECH") }}.DERV_PRTF_PATY_INT_GRUP_PSST B
        On A.PATY_I = B.PATY_I
        And A.INT_GRUP_I = B.INT_GRUP_I
        And A.ROW_N = B.ROW_N + 1
        And A.EFFT_D <> B.EFFT_D
    ) DT1
    On C.INT_GRUP_I = DT1.INT_GRUP_I
    And C.PATY_I = DT1.PATY_I
    And C.INT_GRUP_I = DT1.INT_GRUP_I
    And C.REL_C = DT1.REL_C
    And C.EFFT_D = DT1.EFFT_D
    And C.ROW_N >= DT1.ROW_N
    And DT1.PERD_D <= C.PERD_D 
 
  ) DT2;

Collect Statistics on {{ bteq_var("STARDATADB") }}.DERV_PRTF_PATY_HIST_PSST;


--<================================================>--
--< STEP 4 -- Original DELETE removed: Delete all the original overlap records >--
--<================================================>--

Delete
	A
From 
	 {{ bteq_var("STARDATADB") }}.DERV_PRTF_PATY_PSST A
	,{{ bteq_var("VTECH") }}.DERV_PRTF_PATY_HIST_PSST B
Where  
  A.PATY_I = B.PATY_I
  And A.INT_GRUP_I = B.INT_GRUP_I
  And (A.JOIN_FROM_D,A.JOIN_TO_D) overlaps (B.JOIN_FROM_D,B.JOIN_TO_D)   
;
-- DBT handles table replacement via materialization strategy



--<================================================>--
--< STEP 5 - Replace with updated records          >--
--<================================================>--

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


