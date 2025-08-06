{{ config(materialized='view', alias='appt_pdct_purp', tags=['LdDelFlagAPPT_PDCT_PURPUpd_1']) }}

SELECT
	APPT_PDCT_I
	EXPY_D
	PROS_KEY_EXPY_I 
FROM {{ ref('ApptPdctPurpDS1') }}