{{ config(materialized='incremental', alias='appt_pdct_acct', incremental_strategy='merge', tags=['LdApptPdctFeatPexaUpd']) }}

SELECT
	APPT_PDCT_I
	FEAT_I
	EFFT_D
	SRCE_SYST_C
	EXPY_D
	PROS_KEY_EXPY_I 
FROM {{ ref('TgtApptFeatUpdateDS') }}