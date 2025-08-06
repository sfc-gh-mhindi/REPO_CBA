{{ config(materialized='view', alias='appt_pdct_feat', tags=['LdApptPdctFeatUpd']) }}

SELECT
	APPT_PDCT_I
	FEAT_I
	EXPY_D
	PROS_KEY_EXPY_I 
FROM {{ ref('TgtApptUpdateDS') }}