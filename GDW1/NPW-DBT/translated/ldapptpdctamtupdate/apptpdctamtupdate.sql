{{ config(materialized='view', tags=['LdApptPdctAmtUpdate']) }}

WITH 
_cba__app_csel4_dev_dataset_appt__pdct__amt__u__cse__com__cpo__bus__ncpr__clnt__20101015 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_dev_dataset_appt__pdct__amt__u__cse__com__cpo__bus__ncpr__clnt__20101015")  }})
ApptPdctAmtUpdate AS (
	SELECT APPT_PDCT_I,
		AMT_TYPE_C,
		EFFT_D,
		SRCE_SYST_C,
		EXPY_D,
		PROS_KEY_EXPY_I
	FROM _cba__app_csel4_dev_dataset_appt__pdct__amt__u__cse__com__cpo__bus__ncpr__clnt__20101015
)

SELECT * FROM ApptPdctAmtUpdate