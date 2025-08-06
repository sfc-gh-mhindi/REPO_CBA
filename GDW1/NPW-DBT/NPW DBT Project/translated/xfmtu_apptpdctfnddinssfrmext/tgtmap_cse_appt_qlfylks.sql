{{ config(materialized='view', tags=['XfmTu_ApptPdctFnddInssFrmExt']) }}

WITH 
_cba__app_hlt_dev_lookupset_map__cse__appt__qlfy__sbty__code AS (
	SELECT
	*
	FROM {{ source("","_cba__app_hlt_dev_lookupset_map__cse__appt__qlfy__sbty__code")  }})
TgtMAP_CSE_APPT_QLFYLks AS (
	SELECT SBTY_CODE,
		APPT_QLFY_C
	FROM _cba__app_hlt_dev_lookupset_map__cse__appt__qlfy__sbty__code
)

SELECT * FROM TgtMAP_CSE_APPT_QLFYLks