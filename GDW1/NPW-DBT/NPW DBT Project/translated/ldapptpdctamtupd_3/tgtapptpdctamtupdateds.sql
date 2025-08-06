{{ config(materialized='view', tags=['LdApptPdctAmtUpd_3']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_appt__pdct__amt__3__u__cse__ccc__bus__app__prod__20060101 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_appt__pdct__amt__3__u__cse__ccc__bus__app__prod__20060101")  }})
TgtApptPdctAmtUpdateDS AS (
	SELECT APPT_PDCT_I,
		AMT_TYPE_C,
		EFFT_D,
		EXPY_D,
		PROS_KEY_EXPY_I
	FROM _cba__app_csel4_csel4dev_dataset_appt__pdct__amt__3__u__cse__ccc__bus__app__prod__20060101
)

SELECT * FROM TgtApptPdctAmtUpdateDS