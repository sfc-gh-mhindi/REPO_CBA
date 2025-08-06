{{ config(materialized='incremental', alias='paty_int_grup', incremental_strategy='merge', tags=['LdPatyIntGrupUpd']) }}

SELECT
	INT_GRUP_I
	SRCE_SYST_PATY_INT_GRUP_I
	EFFT_D
	EXPY_D
	PROS_KEY_EXPY_I 
FROM {{ ref('TgtPatyIntGrupUpdateDS') }}