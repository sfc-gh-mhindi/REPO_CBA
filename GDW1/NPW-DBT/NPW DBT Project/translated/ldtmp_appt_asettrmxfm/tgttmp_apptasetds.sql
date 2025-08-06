{{ config(materialized='view', tags=['LdTMP_APPT_ASETTrmXfm']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_tmp__cse__com__bus__ccl__chl__com__app__appt__aset AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_tmp__cse__com__bus__ccl__chl__com__app__appt__aset")  }})
TgtTmp_ApptAsetDS AS (
	SELECT APPT_I,
		ASET_I,
		PRIM_SECU_F,
		EFFT_D,
		EXPY_D,
		EROR_SEQN_I,
		RUN_STRM,
		CHL_PRCP_SCUY_FLAG
	FROM _cba__app_csel4_csel4dev_dataset_tmp__cse__com__bus__ccl__chl__com__app__appt__aset
)

SELECT * FROM TgtTmp_ApptAsetDS