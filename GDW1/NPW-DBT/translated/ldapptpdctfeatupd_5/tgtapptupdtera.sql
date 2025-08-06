{{ config(materialized='incremental', alias='appt_pdct_feat', incremental_strategy='merge', tags=['LdApptPdctFeatUpd_5']) }}

SELECT
	APPT_PDCT_I
	FEAT_I
	EFFT_D
	EXPY_D
	PROS_KEY_EXPY_I 
FROM {{ ref('TgtApptUpdateDS') }}