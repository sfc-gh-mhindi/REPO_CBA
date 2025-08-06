{{ config(materialized='view', tags=['XfmTu_App_Cnd_XFERFrmExt']) }}

WITH 
_cba__app_hlt_dev_lookupset_map__cse__appt__cond AS (
	SELECT
	*
	FROM {{ source("","_cba__app_hlt_dev_lookupset_map__cse__appt__cond")  }})
TgtMAP_CSE_APPT_CONDLks AS (
	SELECT COND_APPT_CAT_ID,
		APPT_COND_C
	FROM _cba__app_hlt_dev_lookupset_map__cse__appt__cond
)

SELECT * FROM TgtMAP_CSE_APPT_CONDLks