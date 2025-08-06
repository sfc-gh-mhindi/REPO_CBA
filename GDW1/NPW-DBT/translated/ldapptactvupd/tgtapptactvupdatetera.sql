{{ config(materialized='incremental', alias='appt_actv', incremental_strategy='merge', tags=['LdApptActvUpd']) }}

SELECT
	APPT_I
	EFFT_D
	EXPY_D
	PROS_KEY_EXPY_I 
FROM {{ ref('Cpy') }}