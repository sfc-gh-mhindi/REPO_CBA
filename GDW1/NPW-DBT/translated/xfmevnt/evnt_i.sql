{{ config(materialized='incremental', alias='_cba__app_csel4_sit_dataset_evnt__i__cse__onln__bus__clnt__rm__rate__20100808', incremental_strategy='insert_overwrite', tags=['XfmEvnt']) }}

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
	ROW_SECU_ACCS_C,
	FNCL_GL_EVNT_F,
	AUTT_AUTN_EVNT_F,
	COLL_EVNT_F 
FROM {{ ref('EvntInserts') }}