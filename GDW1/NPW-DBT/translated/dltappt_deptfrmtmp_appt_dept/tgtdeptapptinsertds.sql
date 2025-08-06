{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_dataset_dept__appt__i__cse__ccc__bus__app__prod__20060101', incremental_strategy='insert_overwrite', tags=['DltAPPT_DEPTFrmTMP_APPT_DEPT']) }}

SELECT
	APPT_I,
	DEPT_ROLE_C,
	EFFT_D,
	DEPT_I,
	EXPY_D,
	PROS_KEY_EFFT_I,
	PROS_KEY_EXPY_I,
	EROR_SEQN_I 
FROM {{ ref('XfmCheckDeltaAction__OutTgtDeptApptInsertDS') }}