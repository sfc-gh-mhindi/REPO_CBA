{{ config(materialized='view', alias='appt_pdct', tags=['LdDelFlagAPPT_PDCTUpd_2']) }}

SELECT
	APPT_I
	APPT_PDCT_I
	EXPY_D
	PROS_KEY_EXPY_I 
FROM {{ ref('ApptPdctDS2') }}