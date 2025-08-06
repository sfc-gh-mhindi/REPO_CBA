{{ config(materialized='view', alias='appt_qstn', tags=['LdAppQstnUpd']) }}

SELECT
	APPT_I
	PATY_I
	QSTN_C
	EXPY_D
	PROS_KEY_EXPY_I 
FROM {{ ref('CPY') }}