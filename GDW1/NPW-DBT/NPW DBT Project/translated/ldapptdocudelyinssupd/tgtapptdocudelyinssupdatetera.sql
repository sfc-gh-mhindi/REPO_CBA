{{ config(materialized='incremental', alias='appt_docu_dely_inss', incremental_strategy='merge', tags=['LdApptDocuDelyInssUpd']) }}

SELECT
	APPT_I
	EFFT_D
	EXPY_D
	PROS_KEY_EXPY_I 
FROM {{ ref('Cpy') }}