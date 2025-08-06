{{ config(materialized='view', tags=['LdTMP_APPT_DEPTrmXfm']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_tmp__cse__com__bus__ccl__chl__com__app__appt__dept AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_tmp__cse__com__bus__ccl__chl__com__app__appt__dept")  }})
SrcApptDeptDS AS (
	SELECT APPT_I,
		DEPT_ROLE_C,
		EFFT_D,
		DEPT_I,
		EXPY_D,
		PROS_KEY_EFFT_I,
		PROS_KEY_EXPY_I,
		EROR_SEQN_I,
		RUN_STRM
	FROM _cba__app_csel4_csel4dev_dataset_tmp__cse__com__bus__ccl__chl__com__app__appt__dept
)

SELECT * FROM SrcApptDeptDS