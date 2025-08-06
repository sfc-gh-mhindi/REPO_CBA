{{ config(materialized='incremental', alias='_cba__app_csel4_dev_dataset_ctct__evnt__i__cse__xs__chl__cpl__bus__app__ans__20080214', incremental_strategy='insert_overwrite', tags=['LdDltAPP_ANS_EVNT']) }}

SELECT
	EVNT_I,
	SRCE_SYST_EVNT_I,
	EVNT_ACTL_D,
	SRCE_SYST_C,
	PROS_KEY_EFFT_I,
	EROR_SEQN_I,
	SRCE_SYST_EVNT_TYPE_I,
	CTCT_EVNT_TYPE_C,
	EVNT_ACTL_T,
	ROW_SECU_ACCS_C 
FROM {{ ref('CTCT_EVNT_Rm_Dup') }}