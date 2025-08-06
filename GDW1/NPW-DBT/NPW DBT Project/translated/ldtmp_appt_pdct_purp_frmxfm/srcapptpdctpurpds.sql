{{ config(materialized='view', tags=['LdTMP_APPT_PDCT_PURP_FrmXfm']) }}

WITH 
_cba__app_csel4_dev_dataset_tmp__cse__chl__bus__hlm__app__appt__pdct__purp AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_dev_dataset_tmp__cse__chl__bus__hlm__app__appt__pdct__purp")  }})
SrcApptPdctPurpDS AS (
	SELECT APPT_PDCT_I,
		EFFT_D,
		SRCE_SYST_APPT_PDCT_PURP_I,
		PURP_TYPE_C,
		PURP_CLAS_C,
		SRCE_SYST_C,
		PURP_A,
		CNCY_C,
		MAIN_PURP_F,
		EXPY_D,
		PROS_KEY_EFFT_I,
		PROS_KEY_EXPY_I,
		EROR_SEQN_I,
		RUN_STRM
	FROM _cba__app_csel4_dev_dataset_tmp__cse__chl__bus__hlm__app__appt__pdct__purp
)

SELECT * FROM SrcApptPdctPurpDS