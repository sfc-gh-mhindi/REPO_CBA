{{ config(materialized='incremental', alias='ctct_evnt', incremental_strategy='append', tags=['LdAPP_ANS_CTCT_EVNT_Ins']) }}

SELECT
	EVNT_I
	SRCE_SYST_EVNT_I
	EVNT_ACTL_D
	SRCE_SYST_C
	PROS_KEY_EFFT_I
	EROR_SEQN_I
	SRCE_SYST_EVNT_TYPE_I
	CTCT_EVNT_TYPE_C
	EVNT_ACTL_T
	ROW_SECU_ACCS_C 
FROM {{ ref('Transformer_105__DSLink106') }}