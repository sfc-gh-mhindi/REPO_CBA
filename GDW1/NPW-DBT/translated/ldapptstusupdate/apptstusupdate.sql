{{ config(materialized='view', tags=['LdApptStusUpdate']) }}

WITH 
_cba__app_csel4_sit_dataset_appt__stus__u__cse__com__cpo__bus__ncpr__clnt__20110203 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_sit_dataset_appt__stus__u__cse__com__cpo__bus__ncpr__clnt__20110203")  }})
ApptStusUpdate AS (
	SELECT APPT_I,
		EFFT_D,
		EXPY_D,
		PROS_KEY_EXPY_I
	FROM _cba__app_csel4_sit_dataset_appt__stus__u__cse__com__cpo__bus__ncpr__clnt__20110203
)

SELECT * FROM ApptStusUpdate