{{ config(materialized='incremental', alias='evnt', incremental_strategy='append', tags=['LdEvntIns']) }}

SELECT
	EVNT_I
	EVNT_ACTV_TYPE_C
	BUSN_EVNT_F
	CTCT_EVNT_F
	INVT_EVNT_F
	FNCL_ACCT_EVNT_F
	FNCL_NVAL_EVNT_F
	INCD_F
	INSR_EVNT_F
	INSR_NVAL_EVNT_F
	ROW_SECU_ACCS_C
	PROS_KEY_EFFT_I
	EROR_SEQN_I 
FROM {{ ref('TgtEvntIInsertDS') }}