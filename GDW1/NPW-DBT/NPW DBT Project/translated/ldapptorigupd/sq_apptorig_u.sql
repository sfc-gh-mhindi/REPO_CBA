{{ config(materialized='view', tags=['LdApptOrigUpd']) }}

WITH 
_cba__app_csel4_dev_outbound_cse__com__bus__ccl__chl__com__app__appt__orig__20130605__u AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_dev_outbound_cse__com__bus__ccl__chl__com__app__appt__orig__20130605__u")  }})
sq_ApptOrig_U AS (
	SELECT APPT_I,
		APPT_ORIG_CATG_C,
		EFFT_D,
		EXPY_D,
		PROS_KEY_EXPY_I
	FROM _cba__app_csel4_dev_outbound_cse__com__bus__ccl__chl__com__app__appt__orig__20130605__u
)

SELECT * FROM sq_ApptOrig_U