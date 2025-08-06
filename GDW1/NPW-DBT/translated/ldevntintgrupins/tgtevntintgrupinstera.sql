{{ config(materialized='incremental', alias='evnt_int_grup', incremental_strategy='append', tags=['LdEvntIntGrupIns']) }}

SELECT
	EVNT_I
	INT_GRUP_I
	EFFT_D
	EXPY_D
	PROS_KEY_EFFT_I
	PROS_KEY_EXPY_I
	EROR_SEQN_I 
FROM {{ ref('TgtEvntIntGrupInsertDS') }}