{{ config(materialized='incremental', alias='int_grup', incremental_strategy='append', tags=['LdIntGrupIns']) }}

SELECT
	INT_GRUP_I
	INT_GRUP_TYPE_C
	INT_GRUP_M
	SRCE_SYST_INT_GRUP_I
	SRCE_SYST_C
	ORIG_SRCE_SYST_INT_GRUP_I
	CRAT_D
	EFFT_D
	EXPY_D
	PROS_KEY_EFFT_I
	PROS_KEY_EXPY_I
	EROR_SEQN_I 
FROM {{ ref('TgtIntGrupInsertDS') }}