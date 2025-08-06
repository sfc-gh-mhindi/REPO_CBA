{{ config(materialized='view', alias='appt_pdct_paty', tags=['LdDelFlagAPPT_PDCT_PATYUpd_1']) }}

SELECT
	APPT_PDCT_I
	PATY_I
	PATY_ROLE_C
	EXPY_D
	PROS_KEY_EXPY_I 
FROM {{ ref('SrcApptPdctPatyDS1') }}