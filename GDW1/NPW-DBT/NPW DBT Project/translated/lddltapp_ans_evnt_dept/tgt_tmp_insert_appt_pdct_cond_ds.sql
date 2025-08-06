{{ config(materialized='incremental', alias='_cba__app_lpxs_lpxs__sit_dataset_evnt__dept__i__cse__xs__chl__cpl__bus__app__ans__20080124', incremental_strategy='insert_overwrite', tags=['LdDltAPP_ANS_EVNT_DEPT']) }}

SELECT
	EVNT_I,
	DEPT_ROLE_C,
	EFFT_D,
	DEPT_I,
	DEPT_RPRT_I,
	TEAM_I,
	EXPY_D,
	PROS_KEY_EFFT_I,
	PROS_KEY_EXPY_I,
	EROR_SEQN_I,
	ROW_SECU_ACCS_C 
FROM {{ ref('XFR_Split__To_Tmp_Insert_USR_TEM_DS') }}