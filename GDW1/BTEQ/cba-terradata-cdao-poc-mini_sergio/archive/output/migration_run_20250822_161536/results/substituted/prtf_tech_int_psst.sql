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
--  1.3  27/08/2013 T Jelliffe         Use only records with same EFFT_D
--  1.4  21/10/2013 T Jelliffe         Insert/Delete changed records
--  1.5  01/11/2013 T Jelliffe         Bug fixes - Row_Number not Rank
------------------------------------------------------------------------------
-- This info is for CBM use only
-- $LastChangedBy: jelifft $
-- $LastChangedDate: 2013-11-01 14:10:34 +1100 (Fri, 01 Nov 2013) $
-- $LastChangedRevision: 12954 $
--

--<================================================>--
--< STEP 4 Delete all the original overlap records >--
--<================================================>--

Delete
	A
From 
	 STAR_CAD_PROD_DATA.DERV_PRTF_INT_PSST A
	,PVTECH.DERV_PRTF_INT_HIST_PSST B
Where  
  A.INT_GRUP_I = B.INT_GRUP_I
  And (A.JOIN_FROM_D,A.JOIN_TO_D) overlaps (B.JOIN_FROM_D,B.JOIN_TO_D)   
;
.IF ERRORCODE <> 0    THEN .GOTO EXITERR

--<===============================================>--
--< STEP 5 - Insert all deduped records into base >--
--<===============================================>--

Insert into STAR_CAD_PROD_DATA.DERV_PRTF_INT_PSST
Select
   A.INT_GRUP_I                    
  ,A.INT_GRUP_TYPE_C               
  ,A.JOIN_FROM_D                   
  ,A.JOIN_TO_D    
  ,A.EFFT_D
  ,A.EXPY_D                 
  ,A.PTCL_N                        
  ,A.REL_MNGE_I                    
  ,A.VALD_FROM_D                   
  ,A.VALD_TO_D          
  ,0 as PROS_KEY_I         
From 
  PVTECH.DERV_PRTF_INT_HIST_PSST A
;
.IF ERRORCODE <> 0    THEN .GOTO EXITERR

Collect Statistics on STAR_CAD_PROD_DATA.DERV_PRTF_INT_PSST;
.IF ERRORCODE <> 0    THEN .GOTO EXITERR 


.QUIT 0
.LOGOFF

.LABEL EXITERR
.QUIT 1
.LOGOFF
.EXIT





