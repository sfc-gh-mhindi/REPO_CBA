{{ config(materialized='incremental', alias='int_grup_unid_paty', incremental_strategy='merge', tags=['LdIntGrupUnidPatyUpd']) }}

SELECT
	INT_GRUP_I
	SRCE_SYST_PATY_I
	EFFT_D
	EXPY_D
	PROS_KEY_EXPY_I 
FROM {{ ref('TgtIntGrupUnidPatyUpdateDS') }}