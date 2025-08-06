{{ config(materialized='incremental', alias='appt_pdct_acct', incremental_strategy='merge', tags=['LdApptFeatLdpUpd']) }}

SELECT
	APPT_I
	EFFT_D
	FEAT_I
	SRCE_SYST_APPT_FEAT_I
	EXPY_D
	PROS_KEY_EXPY_I 
FROM {{ ref('TgtApptFeatUpdateDS') }}