{{ config(materialized='view', tags=['LdCseApptPdctOfiSetl_Ins']) }}

WITH 
_cba__app_csel4_dev_dataset_cse__appt__pdct__ofi__setl__i__cse__chl__bus__hlm__app__20150117 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_dev_dataset_cse__appt__pdct__ofi__setl__i__cse__chl__bus__hlm__app__20150117")  }})
TgtCseApptPdctOfiSetlInsertDS AS (
	SELECT APPT_PDCT_I,
		EFFT_D,
		EXPY_D,
		PROS_KEY_EFFT_I,
		PROS_KEY_EXPY_I,
		DCHG_OFI_IDNN_X,
		DCHG_OFI_M,
		ROW_SECU_ACESS_C
	FROM _cba__app_csel4_dev_dataset_cse__appt__pdct__ofi__setl__i__cse__chl__bus__hlm__app__20150117
)

SELECT * FROM TgtCseApptPdctOfiSetlInsertDS