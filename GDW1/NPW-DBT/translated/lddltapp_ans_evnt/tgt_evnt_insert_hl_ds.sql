{{ config(materialized='incremental', alias='_cba__app_csel4_dev_dataset_evnt__i__cse__xs__chl__cpl__bus__app__ans__20080214', incremental_strategy='insert_overwrite', tags=['LdDltAPP_ANS_EVNT']) }}

SELECT
	EVNT_I,
	EVNT_ACTV_TYPE_C,
	INVT_EVNT_F,
	FNCL_ACCT_EVNT_F,
	CTCT_EVNT_F,
	BUSN_EVNT_F,
	PROS_KEY_EFFT_I,
	EROR_SEQN_I,
	FNCL_NVAL_EVNT_F,
	INCD_F,
	INSR_EVNT_F,
	INSR_NVAL_EVNT_F,
	ROW_SECU_ACCS_C 
FROM {{ ref('EVNT_Rm_Dup_HL') }}