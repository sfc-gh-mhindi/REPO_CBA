-- =====================================================================
-- DBT Model: prtf_tech_int_grup_own_psst
-- Converted from BTEQ: prtf_tech_int_grup_own_psst.sql
-- Category: portfolio_technical
-- Original Size: 11.3KB, 328 lines
-- Complexity Score: 102
-- Generated: 2025-08-21 13:45:01
-- =====================================================================

{{ intermediate_model_config() }}



------------------------------------------------------------------------------
--
--
--  Ver  Date       Modified By        Description
--  ---- ---------- ------------------ ---------------------------------------
--  1.0  11/06/2013 T Jelliffe         Initial Version
--  1.1  11/07/2013 T Jelliffe         Use PROS_KEY_EFFT_I prevent self join
--  1.2  12/07/2013 T Jelliffe         Time period reduced 15 to 3 years
--  1.3  31/07/2013 Z Lwin             Remove EROR_SEQN_I
--  1.4  11/10/2013 T Jelliffe         Fix sum bug in HIST table
--  1.5  01/11/2013 T Jelliffe         Bug fixes - Row_Number not Rank
--  1.6  4/11/2013  T Jelliffe         Duplicates with same VALD dates allowed
------------------------------------------------------------------------------
--
-- This info is for CBM use only
-- $LastChangedBy: jelifft $
-- $LastChangedDate: 2013-11-04 16:25:43 +1100 (Mon, 04 Nov 2013) $
-- $LastChangedRevision: 12958 $
--


--<=========================================================================>--
--< Step 1 : Populate the final OWN_PSST table with source data
--<=========================================================================>--

-- Original -- Original DELETE removed: DELETE removed: Delete from {{ bteq_var("STARDATADB") }}.DERV_PRTF_OWN_PSST All
;
-- DBT handles table replacement via materialization strategy
-- DBT handles table replacement via materialization strategy

-- Original INSERT converted to SELECT for DBT intermediate model
Select
   IGE.INT_GRUP_I                    
  ,IGE.VALD_FROM_D as JOIN_FROM_D                   
  ,Coalesce(IGE.VALD_TO_D, ('9999-12-31' (Date,format'YYYY-MM-DD')) ) as JOIN_TO_D
  ,IGE.VALD_FROM_D                   
  ,IGE.VALD_TO_D
  ,IGE.EFFT_D                        
  ,IGE.EXPY_D                                            
   ,(Case
      When  IGE.EMPL_ROLE_C = 'OWN' Then 'OWNR'
      When  IGE.EMPL_ROLE_C = 'AOW' Then 'AOWN'
      When  IGE.EMPL_ROLE_C = 'AST' Then 'ASTT'      
      Else Null
     End)                  as DERV_PRTF_ROLE_C	               
  ,('Employee'(VARCHAR(40))) as ROLE_PLAY_TYPE_X  
  ,IGE.EMPL_I as ROLE_PLAY_I                        
  ,IGE.SRCE_SYST_C                                       
  ,IGE.ROW_SECU_ACCS_C               
  ,IGE.PROS_KEY_EFFT_I               
From
  {{ bteq_var("VTECH") }}.INT_GRUP_EMPL IGE

 /* Add the GRD filter to reduce the data */
  INNER JOIN {{ bteq_var("VTECH") }}.INT_GRUP IG
  ON IG.INT_GRUP_I = IGE.INT_GRUP_I
    
  INNER JOIN {{ bteq_var("VTECH") }}.GRD_PRTF_TYPE_ENHC_HIST_PSST GPTE  
  ON GPTE.PRTF_TYPE_C = IG.INT_GRUP_TYPE_C
  AND (GPTE.VALD_FROM_D,GPTE.VALD_TO_D) Overlaps (IG.CRAT_D,IG.VALD_TO_D)
  AND (GPTE.VALD_FROM_D,GPTE.VALD_TO_D) Overlaps (IGE.VALD_FROM_D,IGE.VALD_TO_D)
Group By 1,2,3,4,5,6,7,8,9,10,11,12,13


Union All

Select
   IGD.INT_GRUP_I                                   
  ,IGD.VALD_FROM_D as JOIN_FROM_D                   
  ,Coalesce(IGD.VALD_TO_D, ('9999-12-31' (Date,format'YYYY-MM-DD')) ) as JOIN_TO_D              
  ,IGD.VALD_FROM_D                 
  ,IGD.VALD_TO_D                  
  ,IGD.EFFT_D                        
  ,IGD.EXPY_D                        
  ,(Case
      When IGD.DEPT_ROLE_C = 'OWNG' Then 'OWNR'
      When IGD.DEPT_ROLE_C = 'STDP' Then 'STDP'   
      Else Null
    End)                    as DERV_PRTF_ROLE_C                
  ,('Department'(VARCHAR(40))) as ROLE_PLAY_TYPE_X              
  ,IGD.DEPT_I as ROLE_PLAY_I                   
  ,IGD.SRCE_SYST_C                   
  ,IGD.ROW_SECU_ACCS_C       
  ,IGD.PROS_KEY_EFFT_I       
from
{{ bteq_var("VTECH") }}.INT_GRUP_DEPT IGD

 /* Add the GRD filter to reduce the data */
  INNER JOIN {{ bteq_var("VTECH") }}.INT_GRUP IG
  ON IG.INT_GRUP_I = IGD.INT_GRUP_I
    
  INNER JOIN {{ bteq_var("VTECH") }}.GRD_PRTF_TYPE_ENHC_HIST_PSST GPTE  
  ON GPTE.PRTF_TYPE_C = IG.INT_GRUP_TYPE_C
  AND (GPTE.VALD_FROM_D,GPTE.VALD_TO_D) Overlaps (IG.CRAT_D,IG.VALD_TO_D)
  AND (GPTE.VALD_FROM_D,GPTE.VALD_TO_D) Overlaps (IGD.VALD_FROM_D,IGD.VALD_TO_D)

Group By 1,2,3,4,5,6,7,8,9,10,11,12,13;

Collect Statistics on {{ bteq_var("STARDATADB") }}.DERV_PRTF_OWN_PSST
;

--<=========================================================================>--
--< Step 2 : Employee Busniness date deduplication
--<=========================================================================>--

-- Original -- Original DELETE removed: DELETE removed: Delete from {{ bteq_var("STARDATADB") }}.DERV_PRTF_INT_GRUP_OWN_PSST All
;
-- DBT handles table replacement via materialization strategy
-- DBT handles table replacement via materialization strategy

-- Original INSERT converted to SELECT for DBT intermediate model
Select
   DT2.INT_GRUP_I           as INT_GRUP_I
  ,DT2.ROLE_PLAY_I          as ROLE_PLAY_I
  ,DT2.EFFT_D               as EFFT_D
  ,DT2.EXPY_D               as EXPY_D
  ,DT2.VALD_FROM_D          as VALD_FROM_D
  ,DT2.VALD_TO_D            as VALD_TO_D
  ,DT2.PERD_D				        as PERD_D  
  ,DT2.ROLE_PLAY_TYPE_X     as ROLE_PLAY_TYPE_X
  ,DT2.DERV_PRTF_ROLE_C     as DERV_PRTF_ROLE_C
  ,DT2.SRCE_SYST_C          as SRCE_SYST_C
	,ROW_NUMBER() OVER (Partition By DT2.INT_GRUP_I, DT2.ROLE_PLAY_I Order by DT2.PERD_D, DT2.DERV_PRTF_ROLE_C )
  ,DT2.ROW_SECU_ACCS_C      as ROW_SECU_ACCS_C
  ,0                        as PROS_KEY_I
From
	(
    select DERV.*,C.CALENDAR_DATE as PERD_D  from 
    (Select
       A.INT_GRUP_I           as INT_GRUP_I
      ,A.ROLE_PLAY_I
      ,A.DERV_PRTF_ROLE_C  
      --,C.CALENDAR_DATE as PERD_D   
      ,A.EFFT_D               as EFFT_D
      ,A.EXPY_D               as EXPY_D
      ,A.ROLE_PLAY_TYPE_X 
      ,A.VALD_FROM_D
      ,A.VALD_TO_D
      ,A.SRCE_SYST_C          as SRCE_SYST_C
      ,A.ROW_SECU_ACCS_C      as ROW_SECU_ACCS_C
	  ,A.JOIN_FROM_D
	  ,A.JOIN_TO_D
    From
      /* KEY = (INT_GRUP_I, ROLE_PLAY_I) */
      {{ bteq_var("VTECH") }}.DERV_PRTF_OWN_PSST A
      Inner Join {{ bteq_var("VTECH") }}.DERV_PRTF_OWN_PSST  B
      On A.INT_GRUP_I = B.INT_GRUP_I
      And A.ROLE_PLAY_I = B.ROLE_PLAY_I
      And A.ROLE_PLAY_TYPE_X = B.ROLE_PLAY_TYPE_X
      And A.DERV_PRTF_ROLE_C = B.DERV_PRTF_ROLE_C
      And A.DERV_PRTF_ROLE_C = 'OWNR' -- Can have multiple Assistant and other role codes
      And (A.JOIN_FROM_D,A.JOIN_TO_D) overlaps (B.JOIN_FROM_D,B.JOIN_TO_D)   
	  --AND A.INT_GRUP_I<> 'SAPPF2479S96'
	  group by 1,2,3,4,5,6,7,8,9,10,11,12)DERV
      --**** Process all overlap records
      --And (
      --	A.JOIN_FROM_D <> B.JOIN_FROM_D
      --	Or A.JOIN_TO_D <> B.JOIN_TO_D
      --  Or ROLE_PLAY_I <> B.ROLE_PLAY_I
      --)
	   
      Inner Join {{ bteq_var("VTECH") }}.CALENDAR C
      On C.CALENDAR_DATE between DERV.JOIN_FROM_D and DERV.JOIN_TO_D
      And C.CALENDAR_DATE between DATEADD(MONTH, -39, (Current_Date - EXTRACT(DAY from CURRENT_DATE) +1 )) and DATEADD(MONTH, 1, Current_Date)
    --Qualify Row_Number() Over( Partition By A.INT_GRUP_I, A.ROLE_PLAY_I, C.CALENDAR_DATE Order BY A.EFFT_D Desc ) = 1
    Qualify Row_Number() Over( Partition By DERV.INT_GRUP_I, DERV.ROLE_PLAY_TYPE_X, C.CALENDAR_DATE Order BY DERV.EFFT_D Desc,  DERV.ROLE_PLAY_I ) = 1 
	) DT2;

Collect Statistics on {{ bteq_var("STARDATADB") }}.DERV_PRTF_INT_GRUP_OWN_PSST;

--<=========================================================================>--
--< Step 3 : History version of above
--<=========================================================================>--

-- Original -- Original DELETE removed: DELETE removed: Delete from {{ bteq_var("STARDATADB") }}.DERV_PRTF_OWN_HIST_PSST 
;
-- DBT handles table replacement via materialization strategy
-- DBT handles table replacement via materialization strategy

-- Original INSERT converted to SELECT for DBT intermediate model
Select
	 DT3.INT_GRUP_I                    
	,DT3.ROLE_PLAY_I 
  ,DT3.JOIN_FROM_D
  ,(Case
      WHEN DT3.JOIN_TO_D = DATEADD(MONTH, 1, CURRENT_DATE) Then ('9999-12-31'(Date,format'yyyy-mm-dd'))             
      ELSE DT3.JOIN_TO_D
    End
    ) as JOIN_TO_D 
	,DT3.EFFT_D                        
	,DT3.EXPY_D   
	,DT3.VALD_FROM_D
	,DT3.VALD_TO_D  
	,DT3.ROLE_PLAY_TYPE_X              
	,DT3.DERV_PRTF_ROLE_C              
	,DT3.SRCE_SYST_C     
	,DT3.ROW_SECU_ACCS_C               
	,DT3.PROS_KEY_I    
From
  (
    Select Distinct
       DT2.INT_GRUP_I                    
      ,DT2.ROLE_PLAY_I 
      ,MIN(DT2.PERD_D) Over ( Partition By DT2.INT_GRUP_I, DT2.ROLE_PLAY_I, DT2.GRUP_N ) as JOIN_FROM_D
      ,MAX(DT2.PERD_D) Over ( Partition By DT2.INT_GRUP_I, DT2.ROLE_PLAY_I, DT2.GRUP_N ) as JOIN_TO_D	
      ,DT2.EFFT_D                        
      ,DT2.EXPY_D   
      ,DT2.VALD_FROM_D
      ,DT2.VALD_TO_D  
      ,DT2.ROLE_PLAY_TYPE_X              
      ,DT2.DERV_PRTF_ROLE_C              
      ,DT2.SRCE_SYST_C     
      ,DT2.ROW_SECU_ACCS_C               
      ,DT2.PROS_KEY_I                    
    From
      (
        Select
           C.INT_GRUP_I                    
          ,C.ROLE_PLAY_I                   
          ,C.EFFT_D                        
          ,C.EXPY_D   
          ,C.VALD_FROM_D
          ,C.VALD_TO_D  
          ,C.PERD_D                        
          ,C.ROLE_PLAY_TYPE_X              
          ,C.DERV_PRTF_ROLE_C              
          ,C.SRCE_SYST_C     
          ,C.ROW_N
          ,C.ROW_SECU_ACCS_C               
          ,C.PROS_KEY_I                    
          ,MAX(Coalesce( DT1.ROW_N, 0 )) Over (Partition By C.INT_GRUP_I, C.ROLE_PLAY_I, C.PERD_D) as GRUP_N 			
        From
          {{ bteq_var("VTECH") }}.DERV_PRTF_INT_GRUP_OWN_PSST C
          Left Join (
            -- Detect the change in non-key values between rows
            Select
               A.INT_GRUP_I
              ,A.ROLE_PLAY_I
              ,A.DERV_PRTF_ROLE_C
              ,A.ROW_N
              ,A.EFFT_D
              ,A.PERD_D
            From
              {{ bteq_var("VTECH") }}.DERV_PRTF_INT_GRUP_OWN_PSST A
              Inner Join {{ bteq_var("VTECH") }}.DERV_PRTF_INT_GRUP_OWN_PSST B
              On A.INT_GRUP_I = B.INT_GRUP_I
              And A.ROLE_PLAY_I = B.ROLE_PLAY_I
              And A.ROW_N = B.ROW_N + 1
              And (
                A.DERV_PRTF_ROLE_C <> B.DERV_PRTF_ROLE_C
                Or
                A.EFFT_D <> B.EFFT_D
              )
          ) DT1
          On C.INT_GRUP_I = DT1.INT_GRUP_I
          And C.ROLE_PLAY_I = DT1.ROLE_PLAY_I
          And C.EFFT_D = DT1.EFFT_D
          And C.ROW_N >= DT1.ROW_N
          And DT1.PERD_D <= C.PERD_D 

      ) DT2
  ) DT3;

Collect Statistics on {{ bteq_var("STARDATADB") }}.DERV_PRTF_OWN_HIST_PSST;

--<=========================================================================>--
--< Step 4 : Repopulate the final table
--<=========================================================================>--

-- Original DELETE removed: Delete
	A
From 
	 {{ bteq_var("STARDATADB") }}.DERV_PRTF_OWN_PSST A
	,{{ bteq_var("VTECH") }}.DERV_PRTF_OWN_HIST_PSST B
Where  
  A.INT_GRUP_I = B.INT_GRUP_I
  And A.ROLE_PLAY_I = B.ROLE_PLAY_I
  And A.ROLE_PLAY_TYPE_X = B.ROLE_PLAY_TYPE_X
  And A.DERV_PRTF_ROLE_C = B.DERV_PRTF_ROLE_C
  And (A.JOIN_FROM_D,A.JOIN_TO_D) overlaps (B.JOIN_FROM_D,B.JOIN_TO_D)    
;
-- DBT handles table replacement via materialization strategy

-- Original INSERT converted to SELECT for DBT intermediate model
Select
   INT_GRUP_I                    
  ,JOIN_FROM_D                   
  ,(Case
      WHEN JOIN_TO_D = DATEADD(MONTH, 1, CURRENT_DATE) Then ('9999-12-31'(Date,format'yyyy-mm-dd'))             
      ELSE JOIN_TO_D
    End
    ) as JOIN_TO_D   
  ,VALD_FROM_D                   
  ,VALD_TO_D             
  ,EFFT_D                        
  ,EXPY_D                        
  ,DERV_PRTF_ROLE_C              
  ,ROLE_PLAY_TYPE_X              
  ,ROLE_PLAY_I                   
  ,SRCE_SYST_C                   
  ,ROW_SECU_ACCS_C               
  ,PROS_KEY_I                                   
From 
  {{ bteq_var("VTECH") }}.DERV_PRTF_OWN_HIST_PSST;


