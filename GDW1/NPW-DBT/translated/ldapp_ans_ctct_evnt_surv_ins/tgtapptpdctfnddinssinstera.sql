{{ config(materialized='incremental', alias='ctct_evnt_surv', incremental_strategy='append', tags=['LdAPP_ANS_CTCT_EVNT_SURV_Ins']) }}

SELECT
	EVNT_I
	QSTN_C
	EFFT_D
	RESP_C
	RESP_CMMT_X
	EXPY_D
	ROW_SECU_ACCS_C
	PROS_KEY_EFFT_I
	PROS_KEY_EXPY_I
	EROR_SEQN_I 
FROM {{ ref('TgtInsertDS') }}