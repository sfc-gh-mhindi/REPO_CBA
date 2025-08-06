{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_dataset_appt__dept__u__cse__ccc__bus__app__prod__20060101', incremental_strategy='insert_overwrite', tags=['DltAPPT_DEPTFrmTMP_APPT_DEPT']) }}

SELECT
	DEPT_I,
	APPT_I,
	DEPT_ROLE_C,
	EFFT_D,
	EXPY_D,
	PROS_KEY_EXPY_I 
FROM {{ ref('XfmCheckDeltaAction__OutTgtApptDeptUpdateDS') }}