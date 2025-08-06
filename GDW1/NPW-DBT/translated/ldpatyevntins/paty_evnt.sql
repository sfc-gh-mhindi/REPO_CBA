{{ config(materialized='incremental', alias='paty_evnt', incremental_strategy='append', tags=['LdPatyEvntIns']) }}

SELECT
	EVNT_I
	SRCE_SYST_PATY_I
	EVNT_PATY_ROLE_TYPE_C
	SRCE_SYST_C
	EFFT_D
	PATY_I
	EXPY_D
	PROS_KEY_EFFT_I
	PROS_KEY_EXPY_I
	EROR_SEQN_I
	ROW_SECU_ACCS_C 
FROM {{ ref('PATY_EVNT_I') }}