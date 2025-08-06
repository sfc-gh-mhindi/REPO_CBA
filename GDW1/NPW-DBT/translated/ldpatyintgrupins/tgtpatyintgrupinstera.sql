{{ config(materialized='incremental', alias='paty_int_grup', incremental_strategy='append', tags=['LdPatyIntGrupIns']) }}

SELECT
	INT_GRUP_I
	REL_I
	SRCE_SYST_C
	SRCE_SYST_PATY_INT_GRUP_I
	ORIG_SRCE_SYST_PATY_I
	ORIG_SRCE_SYST_PATY_TYPE_C
	PRIM_CLNT_F
	PATY_I
	EFFT_D
	EXPY_D
	PROS_KEY_EFFT_I
	PROS_KEY_EXPY_I
	EROR_SEQN_I 
FROM {{ ref('TgtPatyIntGrupInsertDS') }}