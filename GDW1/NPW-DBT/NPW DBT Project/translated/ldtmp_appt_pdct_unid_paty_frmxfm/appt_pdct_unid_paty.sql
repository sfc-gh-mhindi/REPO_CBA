{{ config(materialized='view', tags=['LdTMP_APPT_PDCT_UNID_PATY_FrmXfm']) }}

WITH 
_cba__app_csel4_dev_dataset_tmp__cse__chl__bus__hlm__app__appt__pdct__unid__paty AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_dev_dataset_tmp__cse__chl__bus__hlm__app__appt__pdct__unid__paty")  }})
APPT_PDCT_UNID_PATY AS (
	SELECT APPT_PDCT_I,
		PATY_ROLE_C,
		SRCE_SYST_PATY_I,
		EFFT_D,
		SRCE_SYST_C,
		UNID_PATY_CATG_C,
		PATY_M,
		EXPY_D,
		PROS_KEY_EFFT_I,
		PROS_KEY_EXPY_I,
		EROR_SEQN_I,
		RUN_STRM
	FROM _cba__app_csel4_dev_dataset_tmp__cse__chl__bus__hlm__app__appt__pdct__unid__paty
)

SELECT * FROM APPT_PDCT_UNID_PATY