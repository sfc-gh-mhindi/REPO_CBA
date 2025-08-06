{{ config(materialized='view', tags=['LdTMP_APPT_PDCT_ACCTFrmXfm']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_tmp__cse__com__bus__ccl__chl__com__app__appt__pdct__acct AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_tmp__cse__com__bus__ccl__chl__com__app__appt__pdct__acct")  }})
SrcApptPdctAcctDS AS (
	SELECT APPT_PDCT_I,
		ACCT_I,
		REL_TYPE_C,
		EFFT_D,
		EXPY_D,
		PROS_KEY_EFFT_I,
		PROS_KEY_EXPY_I,
		EROR_SEQN_I,
		RUN_STRM
	FROM _cba__app_csel4_csel4dev_dataset_tmp__cse__com__bus__ccl__chl__com__app__appt__pdct__acct
)

SELECT * FROM SrcApptPdctAcctDS