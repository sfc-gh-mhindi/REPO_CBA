{{ config(materialized='incremental', alias='appt_paty', incremental_strategy='merge', tags=['LdApptPatyUpd']) }}

SELECT
	APPT_I
	PATY_I
	EFFT_D
	EXPY_D
	PROS_KEY_EXPY_I 
FROM {{ ref('TgtApptAcctUpdateDS') }}