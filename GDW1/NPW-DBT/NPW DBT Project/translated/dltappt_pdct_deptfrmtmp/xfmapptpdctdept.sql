{{ config(materialized='view', tags=['DltAppt_Pdct_DeptFrmTMP']) }}

WITH 
_cba__app_csel4_sit_dataset_tmp__cse__com__cpo__bus__ncpr__clnt__appt__pdct__dept AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_sit_dataset_tmp__cse__com__cpo__bus__ncpr__clnt__appt__pdct__dept")  }})
XfmApptPdctDept AS (
	SELECT APPT_PDCT_I,
		DEPT_I,
		DEPT_ROLE_C,
		SRCE_SYST_C,
		BRCH_N,
		EFFT_D,
		EXPY_D,
		PROS_KEY_EFFT_I,
		PROS_KEY_EXPY_I,
		RUN_STRM
	FROM _cba__app_csel4_sit_dataset_tmp__cse__com__cpo__bus__ncpr__clnt__appt__pdct__dept
)

SELECT * FROM XfmApptPdctDept