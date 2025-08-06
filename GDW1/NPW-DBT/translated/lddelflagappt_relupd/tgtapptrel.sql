{{ config(materialized='view', alias='appt_rel', tags=['LdDelFlagAPPT_RELUpd']) }}

SELECT
	APPT_I
	RELD_APPT_I
	REL_TYPE_C
	EXPY_D
	PROS_KEY_EXPY_I 
FROM {{ ref('ApptRelDS') }}