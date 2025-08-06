{{ config(materialized='incremental', alias='_cba__app_lpxs_lpxs__dev_dataset_evnt__paty__u__cse__xs__chl__cpl__bus__app__ans__20080124', incremental_strategy='insert_overwrite', tags=['LdAPP_ANS_EVNT_PATY_Ins']) }}

SELECT
	EVNT_I,
	SRCE_SYST_PATY_I,
	EVNT_PATY_ROLE_TYPE_C,
	EFFT_D,
	SRCE_SYST_C,
	PATY_I,
	EXPY_D,
	PROS_KEY_EFFT_I,
	PROS_KEY_EXPY_I,
	EROR_SEQN_I,
	ROW_SECU_ACCS_C 
FROM {{ ref('XfmTouchUpdDummy__OutTgtUpdateDS') }}