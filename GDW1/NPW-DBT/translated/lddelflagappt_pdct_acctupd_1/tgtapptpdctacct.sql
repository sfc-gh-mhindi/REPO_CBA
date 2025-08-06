{{ config(materialized='view', alias='appt_pdct_acct', tags=['LdDelFlagAPPT_PDCT_ACCTUpd_1']) }}

SELECT
	APPT_PDCT_I
	EXPY_D
	PROS_KEY_EXPY_I 
FROM {{ ref('ApptPdctAcctDS1') }}