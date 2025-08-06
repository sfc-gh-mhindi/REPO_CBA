{{ config(materialized='view', tags=['LdTMP_APPT_ASETFrmXfm1']) }}

WITH 
_cba__app_csel4_dev_dataset_tmp__cse__chl__bus__hlm__app__sec__2__appt__aset AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_dev_dataset_tmp__cse__chl__bus__hlm__app__sec__2__appt__aset")  }})
TgtTmp_ApptAsetDS AS (
	SELECT APPT_I,
		ASET_I,
		PRIM_SECU_F,
		EFFT_D,
		EXPY_D,
		PROS_KEY_EFFT_I,
		PROS_KEY_EXPY_I,
		eror_seqn_i,
		ASET_SETL_REQD,
		RUN_STRM
	FROM _cba__app_csel4_dev_dataset_tmp__cse__chl__bus__hlm__app__sec__2__appt__aset
)

SELECT * FROM TgtTmp_ApptAsetDS