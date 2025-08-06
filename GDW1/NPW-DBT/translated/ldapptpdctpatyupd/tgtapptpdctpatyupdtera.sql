{{ config(materialized='incremental', alias='appt_pdct_paty', incremental_strategy='merge', tags=['LdApptPdctPatyUpd']) }}

SELECT
	APPT_PDCT_I
	PATY_I
	PATY_ROLE_C
	EFFT_D
	PROS_KEY_EXPY_I
	EXPY_D 
FROM {{ ref('TgtApptPdctPatyUpdateDS') }}