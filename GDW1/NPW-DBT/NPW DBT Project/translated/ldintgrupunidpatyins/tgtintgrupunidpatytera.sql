{{ config(materialized='incremental', alias='int_grup_unid_paty', incremental_strategy='append', tags=['LdIntGrupUnidPatyIns']) }}

SELECT
	INT_GRUP_I
	SRCE_SYST_PATY_I
	ORIG_SRCE_SYST_PATY_I
	UNID_PATY_M
	PATY_TYPE_C
	SRCE_SYST_C
	EFFT_D
	EXPY_D
	PROS_KEY_EFFT_I
	PROS_KEY_EXPY_I
	EROR_SEQN_I 
FROM {{ ref('TgtIntGrupUnidPatyInsertDS') }}