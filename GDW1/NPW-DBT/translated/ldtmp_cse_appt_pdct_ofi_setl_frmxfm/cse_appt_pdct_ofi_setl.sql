{{ config(materialized='view', tags=['LdTMP_CSE_APPT_PDCT_OFI_SETL_FrmXfm']) }}

WITH 
_cba__app_csel4_dev_dataset_tmp__cse__chl__bus__hlm__app__cse__appt__pdct__ofi__setl AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_dev_dataset_tmp__cse__chl__bus__hlm__app__cse__appt__pdct__ofi__setl")  }})
CSE_APPT_PDCT_OFI_SETL AS (
	SELECT APPT_PDCT_I,
		OFI_IDNN_X,
		OFI_M,
		EFFT_D,
		EXPY_D,
		PROS_KEY_EFFT_I,
		PROS_KEY_EXPY_I,
		ROW_SECU_ACCS_C,
		RUN_STREAM
	FROM _cba__app_csel4_dev_dataset_tmp__cse__chl__bus__hlm__app__cse__appt__pdct__ofi__setl
)

SELECT * FROM CSE_APPT_PDCT_OFI_SETL