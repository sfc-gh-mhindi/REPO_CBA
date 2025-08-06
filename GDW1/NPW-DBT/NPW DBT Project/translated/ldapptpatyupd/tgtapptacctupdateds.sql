{{ config(materialized='view', tags=['LdApptPatyUpd']) }}

WITH 
_cba__app_hlt_sit_dataset_appt__paty__u__cse__ccl__bus__app__client__20080818 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_hlt_sit_dataset_appt__paty__u__cse__ccl__bus__app__client__20080818")  }})
TgtApptAcctUpdateDS AS (
	SELECT APPT_I,
		PATY_I,
		EFFT_D,
		EXPY_D,
		PROS_KEY_EXPY_I
	FROM _cba__app_hlt_sit_dataset_appt__paty__u__cse__ccl__bus__app__client__20080818
)

SELECT * FROM TgtApptAcctUpdateDS