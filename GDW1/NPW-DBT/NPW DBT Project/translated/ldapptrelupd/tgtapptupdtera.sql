{{ config(materialized='incremental', alias='appt_rel', incremental_strategy='merge', tags=['LdApptRelUpd']) }}

SELECT
	APPT_I
	RELD_APPT_I
	REL_TYPE_C
	EFFT_D
	EXPY_D
	PROS_KEY_EXPY_I 
FROM {{ ref('TgtApptUpdateDS') }}