{{ config(materialized='view', alias='appt_pdct_rel', tags=['LdDelFlagAPPT_PDCT_RELUpd_1']) }}

SELECT
	APPT_PDCT_I
	RELD_APPT_PDCT_I
	EXPY_D
	PROS_KEY_EXPY_I 
FROM {{ ref('ApptPdctRelDS1') }}