{{ config(materialized='incremental', alias='appt_pdct_acct', incremental_strategy='merge', tags=['LdApptPdctUnidPatyUpd']) }}

SELECT
	APPT_PDCT_I
	PATY_ROLE_C
	EFFT_D
	EXPY_D
	PROS_KEY_EXPY_I 
FROM {{ ref('TgtApptUpdateDS') }}