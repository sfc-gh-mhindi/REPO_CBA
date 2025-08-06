{{ config(materialized='incremental', alias='appt_pdct_acct', incremental_strategy='merge', tags=['LdApptFeatUpd']) }}

SELECT
	APPT_I
	EFFT_D
	SRCE_SYST_APPT_FEAT_I
	EXPY_D
	PROS_KEY_EXPY_I 
FROM {{ ref('TgtApptFeatUpdateDS') }}