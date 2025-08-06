{{ config(materialized='view', tags=['LdApptDocuDelyInssUpd']) }}

WITH 
_cba__app_csel4_dev_dataset_appt__docu__dely__inss__u__cse__chl__bus__app__20100914 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_dev_dataset_appt__docu__dely__inss__u__cse__chl__bus__app__20100914")  }})
TgtApptDocuDelyInssUpdateDS AS (
	SELECT APPT_I,
		EFFT_D,
		EXPY_D,
		PROS_KEY_EXPY_I
	FROM _cba__app_csel4_dev_dataset_appt__docu__dely__inss__u__cse__chl__bus__app__20100914
)

SELECT * FROM TgtApptDocuDelyInssUpdateDS