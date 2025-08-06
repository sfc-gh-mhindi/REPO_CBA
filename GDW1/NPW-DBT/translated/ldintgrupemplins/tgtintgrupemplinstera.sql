{{ config(materialized='incremental', alias='int_grup_empl', incremental_strategy='append', tags=['LdIntGrupEmplIns']) }}

SELECT
	INT_GRUP_I
	EMPL_I
	EMPL_ROLE_C
	EFFT_D
	EXPY_D
	PROS_KEY_EFFT_I
	PROS_KEY_EXPY_I
	EROR_SEQN_I 
FROM {{ ref('TgtIntGrupEmplInsertDS') }}