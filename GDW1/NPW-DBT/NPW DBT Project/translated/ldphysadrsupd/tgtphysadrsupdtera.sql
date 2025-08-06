{{ config(materialized='incremental', alias='phys_adrs', incremental_strategy='merge', tags=['LdPhysAdrsUpd']) }}

SELECT
	ADRS_I
	EFFT_D
	EXPY_D
	PROS_KEY_EXPY_I 
FROM {{ ref('TgtPhysAdrsUpdateDS') }}