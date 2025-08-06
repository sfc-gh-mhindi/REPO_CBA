{{ config(materialized='view', tags=['LdApptPatyIns']) }}

WITH 
_cba__app_hlt_sit_dataset_appt__paty__i__cse__ccl__bus__app__client__20080818 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_hlt_sit_dataset_appt__paty__i__cse__ccl__bus__app__client__20080818")  }})
TgtApptInsertDS AS (
	SELECT APPT_I,
		PATY_I,
		EFFT_D,
		EXPY_D,
		REL_C,
		REL_REAS_C,
		REL_STUS_C,
		REL_LEVL_C,
		SRCE_SYST_C,
		PROS_KEY_EFFT_I,
		PROS_KEY_EXPY_I,
		EROR_SEQN_I
	FROM _cba__app_hlt_sit_dataset_appt__paty__i__cse__ccl__bus__app__client__20080818
)

SELECT * FROM TgtApptInsertDS