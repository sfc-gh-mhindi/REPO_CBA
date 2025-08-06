{{ config(materialized='incremental', alias='_cba__app_csel4_dev_dataset_dept__appt__u__cse__com__cpo__bus__ncpr__clnt__20110107', incremental_strategy='insert_overwrite', tags=['DltAppt_DeptFrmTMP_XFM']) }}

SELECT
	DEPT_I,
	APPT_I,
	DEPT_ROLE_C,
	EFFT_D,
	EXPY_D,
	PROS_KEY_EXPY_I 
FROM {{ ref('XfmCheckDeltaAction__OutTgtDeptApptUpdateDS') }}