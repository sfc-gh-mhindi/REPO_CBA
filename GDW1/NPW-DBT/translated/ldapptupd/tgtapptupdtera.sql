{{ config(materialized='incremental', alias='appt', incremental_strategy='merge', tags=['LdApptUpd']) }}

SELECT
	APPT_I
	APPT_C
	APPT_FORM_C
	STUS_TRAK_I
	APPT_ORIG_C 
FROM {{ ref('TgtApptUpdateDS') }}