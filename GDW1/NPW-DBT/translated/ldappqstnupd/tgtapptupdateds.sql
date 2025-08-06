{{ config(materialized='view', tags=['LdAppQstnUpd']) }}

WITH 
_cba__app_csel4_csel4__prd_dataset_appt__qstn__u__cse__clp__bus__appt__qstn__20080402 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4__prd_dataset_appt__qstn__u__cse__clp__bus__appt__qstn__20080402")  }})
TgtApptUpdateDS AS (
	SELECT APPT_I,
		PATY_I,
		QSTN_C,
		EXPY_D,
		PROS_KEY_EXPY_I
	FROM _cba__app_csel4_csel4__prd_dataset_appt__qstn__u__cse__clp__bus__appt__qstn__20080402
)

SELECT * FROM TgtApptUpdateDS