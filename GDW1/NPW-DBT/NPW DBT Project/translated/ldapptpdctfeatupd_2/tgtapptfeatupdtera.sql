{{ config(materialized='incremental', alias='appt_pdct_acct', incremental_strategy='merge', tags=['LdApptPdctFeatUpd_2']) }}

SELECT
	APPT_PDCT_I
	FEAT_I
	SRCE_SYST_APPT_FEAT_I
	EFFT_D
	EXPY_D
	PROS_KEY_EXPY_I 
FROM {{ ref('TgtApptFeatUpdateDS') }}