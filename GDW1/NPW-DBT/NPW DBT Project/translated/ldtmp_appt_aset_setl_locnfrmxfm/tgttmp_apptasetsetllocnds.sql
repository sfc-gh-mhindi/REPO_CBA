{{ config(materialized='view', tags=['LdTMP_APPT_ASET_SETL_LOCNFrmXfm']) }}

WITH 
_cba__app_csel4_dev_dataset_tmp__cse__chl__bus__hlm__app__sec__appt__aset__setl__locn AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_dev_dataset_tmp__cse__chl__bus__hlm__app__sec__appt__aset__setl__locn")  }})
TgtTmp_ApptAsetSetlLocnDS AS (
	SELECT APPT_I,
		ASET_I,
		SRCE_SYST_C,
		FRWD_DOCU_C,
		SETL_LOCN_X,
		SETL_CMMT_X,
		EFFT_D,
		EXPY_D,
		RUN_STRM
	FROM _cba__app_csel4_dev_dataset_tmp__cse__chl__bus__hlm__app__sec__appt__aset__setl__locn
)

SELECT * FROM TgtTmp_ApptAsetSetlLocnDS