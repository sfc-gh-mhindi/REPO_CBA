{{ config(materialized='view', tags=['LdTMP_APPT_PDCT_RPAYFrmXfm']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_tmp__cse__com__bus__ccl__chl__com__app__appt__pdct__rpay AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_tmp__cse__com__bus__ccl__chl__com__app__appt__pdct__rpay")  }})
SrcApptPdctRpayDS AS (
	SELECT APPT_PDCT_I,
		RPAY_TYPE_C,
		EFFT_D,
		PAYT_FREQ_C,
		STRT_RPAY_D,
		EXPY_D,
		PROS_KEY_EFFT_I,
		PROS_KEY_EXPY_I,
		EROR_SEQN_I,
		RUN_STRM
	FROM _cba__app_csel4_csel4dev_dataset_tmp__cse__com__bus__ccl__chl__com__app__appt__pdct__rpay
)

SELECT * FROM SrcApptPdctRpayDS