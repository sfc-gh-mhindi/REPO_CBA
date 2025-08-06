{{ config(materialized='view', alias='appt_pdct_amt', tags=['LdDelFlagAPPT_PDCT_AMTUpd_2']) }}

SELECT
	APPT_PDCT_I
	EXPY_D
	PROS_KEY_EXPY_I 
FROM {{ ref('ApptPdctAmtDS2') }}