{{ config(materialized='view', tags=['LdCseApptPdctOfiSetl_Upd']) }}

WITH 
_cba__app_csel4_dev_dataset_cse__appt__pdct__ofi__setl__u__cse__chl__bus__hlm__app__20150117 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_dev_dataset_cse__appt__pdct__ofi__setl__u__cse__chl__bus__hlm__app__20150117")  }})
TgtCseApptPdctOfiSetlUpdateDS AS (
	SELECT APPT_PDCT_I,
		EFFT_D,
		EXPY_D,
		PROS_KEY_EXPY_I
	FROM _cba__app_csel4_dev_dataset_cse__appt__pdct__ofi__setl__u__cse__chl__bus__hlm__app__20150117
)

SELECT * FROM TgtCseApptPdctOfiSetlUpdateDS