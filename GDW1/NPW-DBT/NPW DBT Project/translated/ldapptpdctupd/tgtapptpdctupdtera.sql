{{ config(materialized='incremental', alias='appt_pdct_acct', incremental_strategy='merge', tags=['LdApptPdctUpd']) }}

SELECT
	APPT_PDCT_I
	APPT_I
	EFFT_D
	EXPY_D
	PROS_KEY_EXPY_I 
FROM {{ ref('TgtApptPdctUpdateDS') }}