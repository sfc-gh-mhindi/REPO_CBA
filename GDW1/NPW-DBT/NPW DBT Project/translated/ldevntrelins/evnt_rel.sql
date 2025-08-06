{{ config(materialized='incremental', alias='evnt_rel', incremental_strategy='append', tags=['LdEvntRelIns']) }}

SELECT
	EVNT_I
	RELD_EVNT_I
	EFFT_D
	EVNT_REL_TYPE_C
	EXPY_D
	PROS_KEY_EFFT_I
	PROS_KEY_EXPY_I
	EROR_SEQN_I
	ROW_SECU_ACCS_C 
FROM {{ ref('EVNT_REL_I') }}