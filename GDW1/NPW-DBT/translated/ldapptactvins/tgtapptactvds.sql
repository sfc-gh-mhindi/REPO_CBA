{{ config(materialized='view', tags=['LdApptActvIns']) }}

WITH 
_cba__app_csel4_dev_dataset_appt__actv__i__cse__chl__bus__app__20100915 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_dev_dataset_appt__actv__i__cse__chl__bus__app__20100915")  }})
TgtApptActvDS AS (
	SELECT APPT_I,
		APPT_ACTV_TYPE_C,
		SRCE_SYST_C,
		APPT_ACTV_Q,
		EFFT_D,
		EXPY_D,
		PROS_KEY_EFFT_I,
		PROS_KEY_EXPY_I,
		ROW_SECU_ACCS_C
	FROM _cba__app_csel4_dev_dataset_appt__actv__i__cse__chl__bus__app__20100915
)

SELECT * FROM TgtApptActvDS