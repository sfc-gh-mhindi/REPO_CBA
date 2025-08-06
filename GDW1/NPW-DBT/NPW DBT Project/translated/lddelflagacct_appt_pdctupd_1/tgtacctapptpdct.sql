{{ config(materialized='view', alias='acct_appt_pdct', tags=['LdDelFlagACCT_APPT_PDCTUpd_1']) }}

SELECT
	ACCT_I
	APPT_PDCT_I
	EXPY_D
	PROS_KEY_EXPY_I 
FROM {{ ref('AcctApptPdctDS1') }}