{{ config(materialized='incremental', alias='_cba__app_lpxs_lpxs__dev_dataset_ctct__evnt__u__cse__xs__chl__cpl__bus__app__ans__20080214', incremental_strategy='insert_overwrite', tags=['LdAPP_ANS_CTCT_EVNT_Ins']) }}

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
FROM {{ ref('Transformer_105__DSLink108') }}