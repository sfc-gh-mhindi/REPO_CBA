{{ config(materialized='view', tags=['LdApptOrigIns']) }}

WITH 
_cba__app_csel4_dev_outbound_cse__com__bus__ccl__chl__com__app__appt__orig__20130605__i AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_dev_outbound_cse__com__bus__ccl__chl__com__app__appt__orig__20130605__i")  }})
sq_ApptOrig_I AS (
	SELECT APPT_I,
		EFFT_D,
		EXPY_D,
		PROS_KEY_EFFT_I,
		PROS_KEY_EXPY_I,
		ROW_SECU_ACCS_C,
		SRCE_SYST_C,
		APPT_ORIG_C,
		APPT_ORIG_CATG_C
	FROM _cba__app_csel4_dev_outbound_cse__com__bus__ccl__chl__com__app__appt__orig__20130605__i
)

SELECT * FROM sq_ApptOrig_I