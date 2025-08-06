{{ config(materialized='view', tags=['LdTMP_APPT_PATYTrmXfm']) }}

WITH 
_cba__app_hlt_sit_dataset_tmp__cse__ccl__bus__app__client__appt__paty AS (
	SELECT
	*
	FROM {{ source("","_cba__app_hlt_sit_dataset_tmp__cse__ccl__bus__app__client__appt__paty")  }})
TgtCclAppPremapDS AS (
	SELECT APPT_I,
		PATY_I,
		REL_C,
		REL_REAS_C,
		REL_STUS_C,
		REL_LEVL_C,
		SRCE_SYST_C,
		RUN_STRM
	FROM _cba__app_hlt_sit_dataset_tmp__cse__ccl__bus__app__client__appt__paty
)

SELECT * FROM TgtCclAppPremapDS