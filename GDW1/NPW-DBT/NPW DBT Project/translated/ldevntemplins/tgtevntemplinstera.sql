{{ config(materialized='incremental', alias='evnt_empl', incremental_strategy='append', tags=['LdEvntEmplIns']) }}

SELECT
	EVNT_I
	ROW_SECU_ACCS_C
	EMPL_I
	EVNT_PATY_ROLE_TYPE_C
	EFFT_D
	EXPY_D
	PROS_KEY_EFFT_I
	PROS_KEY_EXPY_I
	EROR_SEQN_I 
FROM {{ ref('TgtEvntEmplInsertDS') }}