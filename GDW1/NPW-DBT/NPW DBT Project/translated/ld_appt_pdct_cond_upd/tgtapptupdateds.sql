{{ config(materialized='view', tags=['Ld_APPT_PDCT_COND_Upd']) }}

WITH 
_cba__app_hlt_dev_dataset_appt__pdct__cond__u__cse__chl__bus__tu__app__cond__20071024 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_hlt_dev_dataset_appt__pdct__cond__u__cse__chl__bus__tu__app__cond__20071024")  }})
TgtApptUpdateDS AS (
	SELECT APPT_PDCT_I,
		COND_C,
		EXPY_D,
		PROS_KEY_EXPY_I
	FROM _cba__app_hlt_dev_dataset_appt__pdct__cond__u__cse__chl__bus__tu__app__cond__20071024
)

SELECT * FROM TgtApptUpdateDS