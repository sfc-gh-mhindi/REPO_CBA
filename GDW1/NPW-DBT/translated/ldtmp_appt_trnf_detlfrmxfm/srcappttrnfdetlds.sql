{{ config(materialized='view', tags=['LdTMP_APPT_TRNF_DETLFrmXfm']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_tmp__cse__com__bus__ccl__chl__com__app__appt__trnf__detl AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_tmp__cse__com__bus__ccl__chl__com__app__appt__trnf__detl")  }})
SrcApptTrnfDetlDS AS (
	SELECT APPT_I,
		APPT_TRNF_I,
		EFFT_D,
		TRNF_OPTN_C,
		TRNF_A,
		CNCY_C,
		CMPE_I,
		EXPY_D,
		PROS_KEY_EFFT_I,
		PROS_KEY_EXPY_I,
		EROR_SEQN_I,
		RUN_STRM
	FROM _cba__app_csel4_csel4dev_dataset_tmp__cse__com__bus__ccl__chl__com__app__appt__trnf__detl
)

SELECT * FROM SrcApptTrnfDetlDS