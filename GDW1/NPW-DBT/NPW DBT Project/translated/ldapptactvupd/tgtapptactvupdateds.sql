{{ config(materialized='view', tags=['LdApptActvUpd']) }}

WITH 
_cba__app_csel4_dev_dataset_appt__actv__u__cse__chl__bus__app__20100915 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_dev_dataset_appt__actv__u__cse__chl__bus__app__20100915")  }})
TgtApptActvUpdateDS AS (
	SELECT APPT_I,
		EFFT_D,
		EXPY_D,
		PROS_KEY_EXPY_I
	FROM _cba__app_csel4_dev_dataset_appt__actv__u__cse__chl__bus__app__20100915
)

SELECT * FROM TgtApptActvUpdateDS