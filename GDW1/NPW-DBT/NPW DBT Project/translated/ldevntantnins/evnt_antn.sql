{{ config(materialized='incremental', alias='evnt_antn', incremental_strategy='append', tags=['LdEvntAntnIns']) }}

SELECT
	EVNT_I
	ANTN_I
	SRCE_SYST_C
	EFFT_D
	EXPY_D
	PROS_KEY_EFFT_I
	PROS_KEY_EXPY_I
	EROR_SEQN_I
	ROW_SECU_ACCS_C 
FROM {{ ref('EVNT_ANTN_I') }}