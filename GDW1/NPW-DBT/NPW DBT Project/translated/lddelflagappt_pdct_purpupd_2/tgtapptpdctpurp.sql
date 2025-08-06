{{ config(materialized='view', alias='appt_pdct_purp', tags=['LdDelFlagAPPT_PDCT_PURPUpd_2']) }}

SELECT
	APPT_PDCT_I
	SRCE_SYST_APPT_PDCT_PURP_I
	EXPY_D
	PROS_KEY_EXPY_I 
FROM {{ ref('ApptPdctPurpDS2') }}