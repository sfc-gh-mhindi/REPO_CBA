{{ config(materialized='view', tags=['LdTMP_CSE_CMLN_APPT_XPOS_ASES_DETLFrmXfm']) }}

WITH 
_cba__app_pj__gdw2project_csel4_dev_dataset_tmp__cse__ccl__bus__app__cse__cmln__appt__xpos__ases__detl AS (
	SELECT
	*
	FROM {{ source("","_cba__app_pj__gdw2project_csel4_dev_dataset_tmp__cse__ccl__bus__app__cse__cmln__appt__xpos__ases__detl")  }})
TmpCclCseCmlnApptXposAsesDetll AS (
	SELECT APPT_I,
		XPOS_A,
		XPOS_AMT_D,
		OVRD_COVTS_ASES_F,
		CSE_CMLN_OVRD_REAS_CATG_C,
		SHRT_DFLT_OVRD_F,
		EFFT_D,
		EXPY_D
	FROM _cba__app_pj__gdw2project_csel4_dev_dataset_tmp__cse__ccl__bus__app__cse__cmln__appt__xpos__ases__detl
)

SELECT * FROM TmpCclCseCmlnApptXposAsesDetll