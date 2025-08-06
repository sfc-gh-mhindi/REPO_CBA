{{ config(materialized='incremental', alias='unid_paty_gnrc_prfl', incremental_strategy='merge', tags=['LdUnidPatyGnrcPrflUpd']) }}

SELECT
	UNID_PATY_I
	EFFT_D
	EXPY_D
	PROS_KEY_EXPY_I 
FROM {{ ref('Cpy') }}