{{ config(materialized='incremental', alias='_cba__app_csel4_dev_dataset_appt__dept__i__cse__com__cpo__bus__ncpr__clnt__20110107', incremental_strategy='insert_overwrite', tags=['DltAppt_DeptFrmTMP_XFM']) }}

SELECT
	APPT_I,
	DEPT_ROLE_C,
	EFFT_D,
	DEPT_I,
	EXPY_D,
	PROS_KEY_EFFT_I,
	PROS_KEY_EXPY_I,
	EROR_SEQN_I 
FROM {{ ref('XfmCheckDeltaAction__OutTgtApptDeptInsertDS') }}