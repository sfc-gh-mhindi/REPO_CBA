{{ config(materialized='view', tags=['LdTMP_APPT_PDCT_PATYFrmXfm']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_tmp__cse__com__bus__ccl__chl__com__app__appt__pdct__paty AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_tmp__cse__com__bus__ccl__chl__com__app__appt__pdct__paty")  }})
SrcApptPdctPatyDS AS (
	SELECT APPT_PDCT_I,
		PATY_I,
		PATY_ROLE_C,
		EFFT_D,
		SRCE_SYST_C,
		SRCE_SYST_APPT_PDCT_PATY_I,
		EXPY_D,
		PROS_KEY_EFFT_I,
		PROS_KEY_EXPY_I,
		EROR_SEQN_I,
		RUN_STRM
	FROM _cba__app_csel4_csel4dev_dataset_tmp__cse__com__bus__ccl__chl__com__app__appt__pdct__paty
)

SELECT * FROM SrcApptPdctPatyDS