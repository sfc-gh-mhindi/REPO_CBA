{{ config(materialized='view', alias='appt_stus', tags=['LdApptStusUpdate']) }}

SELECT
	APPT_I
	EFFT_D
	EXPY_D
	PROS_KEY_EXPY_I 
FROM {{ ref('ApptStusUpdate') }}