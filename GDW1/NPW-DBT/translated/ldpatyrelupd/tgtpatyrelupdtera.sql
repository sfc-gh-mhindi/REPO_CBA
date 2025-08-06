{{ config(materialized='incremental', alias='paty_rel', incremental_strategy='merge', tags=['LdPatyRelUpd']) }}

SELECT
	REL_I
	PATY_I
	RELD_PATY_I
	REL_LEVL_C
	EFFT_D
	EXPY_D
	PROS_KEY_EXPY_I 
FROM {{ ref('TgtPatyRelUpdateDS') }}