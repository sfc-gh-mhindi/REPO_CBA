{{ config(materialized='incremental', alias='appt_pdct_acct', incremental_strategy='merge', tags=['LdApptPdctPurpUpd']) }}

SELECT
	APPT_PDCT_I
	EFFT_D
	SRCE_SYST_APPT_PDCT_PURP_I
	EXPY_D
	PROS_KEY_EXPY_I 
FROM {{ ref('TgtApptPdctPurpUpdateDS') }}