{{ config(materialized='view', alias='appt_pdct_cond', tags=['Ld_APPT_PDCT_COND_Upd']) }}

SELECT
	APPT_PDCT_I
	COND_C
	EXPY_D
	PROS_KEY_EXPY_I 
FROM {{ ref('TgtApptUpdateDS') }}