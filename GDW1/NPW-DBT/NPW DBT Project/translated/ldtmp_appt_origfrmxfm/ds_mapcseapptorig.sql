{{ config(materialized='view', tags=['LdTMP_APPT_ORIGFrmXfm']) }}

WITH 
_cba__app_csel4_dev_dataset_cse__com__bus__ccl__chl__com__app__map__cse__appt__orig__20130705 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_dev_dataset_cse__com__bus__ccl__chl__com__app__map__cse__appt__orig__20130705")  }})
ds_MapCseApptOrig AS (
	SELECT CHNL_CAT_ID,
		APPT_ORIG_C,
		EFFT_D,
		EXPY_D
	FROM _cba__app_csel4_dev_dataset_cse__com__bus__ccl__chl__com__app__map__cse__appt__orig__20130705
)

SELECT * FROM ds_MapCseApptOrig