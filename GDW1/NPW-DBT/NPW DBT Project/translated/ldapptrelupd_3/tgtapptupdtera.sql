{{ config(materialized='incremental', alias='appt_rel', incremental_strategy='merge', tags=['LdApptRelUpd_3']) }}

SELECT
	APPT_I
	REL_TYPE_C
	EFFT_D
	EXPY_D
	PROS_KEY_EXPY_I 
FROM {{ ref('TgtApptUpdateDS') }}