{{ config(materialized='view', tags=['LdTMP_APPT_ACTVFrmXfm']) }}

WITH 
_cba__app_csel4_dev_dataset_tmp__cse__chl__bus__app__appt__actv AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_dev_dataset_tmp__cse__chl__bus__app__appt__actv")  }})
TgtTmpApptActvDS AS (
	SELECT APPT_I,
		APPT_ACTV_Q,
		RUN_STRM
	FROM _cba__app_csel4_dev_dataset_tmp__cse__chl__bus__app__appt__actv
)

SELECT * FROM TgtTmpApptActvDS