{{ config(materialized='view', tags=['LdCseCmlnApptXposAsesDetlIns']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_appt__pdct__acct__i__cse__ccc__bus__app__prod__20060101 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_appt__pdct__acct__i__cse__ccc__bus__app__prod__20060101")  }})
TgtCseCmlnApptXposAsesDetlInsertDS AS (
	SELECT APPT_I,
		XPOS_A,
		XPOS_AMT_D,
		OVRD_COVTS_ASES_F,
		CSE_CMLN_OVRD_REAS_CATG_C,
		SHRT_DFLT_OVRD_F,
		EFFT_D,
		EXPY_D,
		PROS_KEY_EFFT_I,
		PROS_KEY_EXPY_I
	FROM _cba__app_csel4_csel4dev_dataset_appt__pdct__acct__i__cse__ccc__bus__app__prod__20060101
)

SELECT * FROM TgtCseCmlnApptXposAsesDetlInsertDS