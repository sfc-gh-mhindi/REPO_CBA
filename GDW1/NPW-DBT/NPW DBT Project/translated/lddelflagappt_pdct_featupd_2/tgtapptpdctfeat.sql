{{ config(materialized='view', alias='appt_pdct_feat', tags=['LdDelFlagAPPT_PDCT_FEATUpd_2']) }}

SELECT
	APPT_PDCT_I
	EXPY_D
	PROS_KEY_EXPY_I 
FROM {{ ref('CC_ApptPdctFeatDS') }}