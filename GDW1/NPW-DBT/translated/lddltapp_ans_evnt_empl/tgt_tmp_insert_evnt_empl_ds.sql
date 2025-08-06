{{ config(materialized='incremental', alias='_cba__app_lpxs_lpxs__dev_dataset_evnt__empl__i__cse__xs__chl__cpl__bus__app__ans__20080214', incremental_strategy='insert_overwrite', tags=['LdDltAPP_ANS_EVNT_EMPL']) }}

SELECT
	EVNT_I,
	EMPL_I,
	EVNT_PATY_ROLE_TYPE_C,
	EFFT_D,
	EXPY_D,
	PROS_KEY_EFFT_I,
	PROS_KEY_EXPY_I,
	EROR_SEQN_I,
	ROW_SECU_ACCS_C 
FROM {{ ref('XFR_Split__To_Tmp_Insert_USR_TEM_DS') }}