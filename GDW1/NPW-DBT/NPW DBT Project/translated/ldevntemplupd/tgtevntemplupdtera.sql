{{ config(materialized='incremental', alias='evnt_empl', incremental_strategy='merge', tags=['LdEvntEmplUpd']) }}

SELECT
	EVNT_I
	EMPL_I
	EFFT_D
	EXPY_D
	PROS_KEY_EXPY_I 
FROM {{ ref('TgtEvntEmplUpdateDS') }}