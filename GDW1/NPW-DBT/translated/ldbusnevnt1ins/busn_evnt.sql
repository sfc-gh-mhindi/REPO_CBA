{{ config(materialized='incremental', alias='busn_evnt', incremental_strategy='append', tags=['LdBusnEvnt1Ins']) }}

SELECT
	EVNT_I
	SRCE_SYST_EVNT_I
	EVNT_ACTL_D
	SRCE_SYST_C
	PROS_KEY_EFFT_I
	EROR_SEQN_I
	SRCE_SYST_EVNT_TYPE_I
	EVNT_ACTL_T
	ROW_SECU_ACCS_C 
FROM {{ ref('BUSN_EVNT_I') }}