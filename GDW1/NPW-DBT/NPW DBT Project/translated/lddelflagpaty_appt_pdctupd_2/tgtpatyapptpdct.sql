{{ config(materialized='view', alias='paty_appt_pdct', tags=['LdDelFlagPATY_APPT_PDCTUpd_2']) }}

SELECT
	PATY_I
	APPT_PDCT_I
	PATY_ROLE_C
	EXPY_D
	PROS_KEY_EXPY_I 
FROM {{ ref('PatyApptPdctDS') }}