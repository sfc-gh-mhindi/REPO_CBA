.RUN FILE=%%BTEQ_LOGON_SCRIPT%%
.IF ERRORCODE <> 0    THEN .GOTO EXITERR

.SET QUIET OFF
.SET ECHOREQ ON
.SET FORMAT OFF
.SET WIDTH 120
                                             
------------------------------------------------------------------------------
--
--
--  Ver  Date       Modified By            Description
--  ---- ---------- ---------------------- -----------------------------------
--  1.0  15/07/2013 T Jelliffe             Initial Version
--  1.5  01/11/2013 T Jelliffe             Add the HIST persisted table
------------------------------------------------------------------------------

-- PDGRD DATA

Delete from %%DGRDDB%%.GRD_PRTF_TYPE_ENHC_PSST All
;
.IF ERRORCODE <> 0    THEN .GOTO EXITERR

Collect Statistics on %%DGRDDB%%.GRD_PRTF_TYPE_ENHC_PSST;
.IF ERRORCODE <> 0    THEN .GOTO EXITERR

Insert into %%DGRDDB%%.GRD_PRTF_TYPE_ENHC_PSST
Select
   GP.PERD_D
  ,GP.PRTF_TYPE_C
  ,GP.PRTF_TYPE_M
  ,GP.PRTF_CLAS_C
  ,GP.PRTF_CLAS_M
  ,GP.PRTF_CATG_C
  ,GP.PRTF_CATG_M
From                
  %%VTECH%%.GRD_PRTF_TYPE_ENHC GP
;
.IF ERRORCODE <> 0    THEN .GOTO EXITERR

Collect Statistics on %%DGRDDB%%.GRD_PRTF_TYPE_ENHC_PSST;
.IF ERRORCODE <> 0    THEN .GOTO EXITERR



--< Populate the HISTORY version of the table >--
Delete from %%DGRDDB%%.GRD_PRTF_TYPE_ENHC_HIST_PSST
;
.IF ERRORCODE <> 0    THEN .GOTO EXITERR

Insert into %%DGRDDB%%.GRD_PRTF_TYPE_ENHC_HIST_PSST
Select
   G.PRTF_TYPE_C                   
  ,G.PRTF_TYPE_M                   
  ,G.PRTF_CLAS_C                   
  ,G.PRTF_CLAS_M                   
  ,G.PRTF_CATG_C                   
  ,G.PRTF_CATG_M    
	,MIN(PERD_D) as VALD_FROM_D
  ,MAX(PERD_D) as VALD_TO_D
From
  %%VTECH%%.GRD_PRTF_TYPE_ENHC_PSST G
Group By 1,2,3,4,5,6
;
.IF ERRORCODE <> 0    THEN .GOTO EXITERR

Collect Statistics on %%DGRDDB%%.GRD_PRTF_TYPE_ENHC_HIST_PSST;
.IF ERRORCODE <> 0    THEN .GOTO EXITERR




.QUIT 0
.LOGOFF

.LABEL EXITERR
.QUIT 1
.LOGOFF
.EXIT 
 
