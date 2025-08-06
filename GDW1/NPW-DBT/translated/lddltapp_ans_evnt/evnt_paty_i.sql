{{ config(materialized='incremental', alias='_cba__app_csel4_dev_dataset_evnt__paty__i__cse__xs__chl__cpl__bus__app__ans__20080214', incremental_strategy='insert_overwrite', tags=['LdDltAPP_ANS_EVNT']) }}

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
FROM {{ ref('Remove_Duplicates_Evnt_paty') }}