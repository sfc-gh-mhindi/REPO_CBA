{{ config(materialized='view', alias='appt_feat', tags=['LdDelFlagAPPT_FEATUpd']) }}

SELECT
	APPT_I
	SRCE_SYST_APPT_FEAT_I
	EXPY_D
	PROS_KEY_EXPY_I 
FROM {{ ref('ApptFeatDS') }}