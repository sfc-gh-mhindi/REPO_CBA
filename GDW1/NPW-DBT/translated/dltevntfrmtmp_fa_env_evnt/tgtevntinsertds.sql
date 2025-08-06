{{ config(materialized='incremental', alias='_cba__app_csel4_dev_dataset_evnt__i__cse__coi__bus__envi__evnt__20150610', incremental_strategy='insert_overwrite', tags=['DltEVNTFrmTMP_FA_ENV_EVNT']) }}

SELECT
	EVNT_I,
	EVNT_ACTV_TYPE_C,
	BUSN_EVNT_F,
	CTCT_EVNT_F,
	INVT_EVNT_F,
	FNCL_ACCT_EVNT_F,
	FNCL_NVAL_EVNT_F,
	INCD_F,
	INSR_EVNT_F,
	INSR_NVAL_EVNT_F,
	ROW_SECU_ACCS_C,
	PROS_KEY_EFFT_I,
	EROR_SEQN_I 
FROM {{ ref('XfmCheckDeltaAction__OutTgtEvntInsertDS') }}