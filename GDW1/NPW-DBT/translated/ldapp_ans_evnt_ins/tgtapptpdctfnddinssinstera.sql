{{ config(materialized='incremental', alias='evnt', incremental_strategy='append', tags=['LdAPP_ANS_EVNT_Ins']) }}

SELECT
	EVNT_I
	EVNT_ACTV_TYPE_C
	INVT_EVNT_F
	FNCL_ACCT_EVNT_F
	CTCT_EVNT_F
	BUSN_EVNT_F
	PROS_KEY_EFFT_I
	EROR_SEQN_I
	FNCL_NVAL_EVNT_F
	INCD_F
	INSR_EVNT_F
	INSR_NVAL_EVNT_F
	ROW_SECU_ACCS_C 
FROM {{ ref('Transformer_105__DSLink106') }}