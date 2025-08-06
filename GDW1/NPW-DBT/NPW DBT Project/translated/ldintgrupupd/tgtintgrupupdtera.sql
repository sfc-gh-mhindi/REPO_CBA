{{ config(materialized='incremental', alias='int_grup', incremental_strategy='merge', tags=['LdIntGrupUpd']) }}

SELECT
	INT_GRUP_I
	EFFT_D
	EXPY_D
	PROS_KEY_EXPY_I 
FROM {{ ref('TgtIntGrupUpdateDS') }}