{{ config(materialized='incremental', alias='appt', incremental_strategy='merge', tags=['LdAppt_TPBUpd']) }}

SELECT
	APPT_I
	APPT_C
	APPT_FORM_C
	STUS_TRAK_I
	APPT_ORIG_C
	ORIG_APPT_SRCE_C
	APPT_RECV_S
	REL_MGR_STAT_C
	APPT_RECV_D
	APPT_RECV_T 
FROM {{ ref('TgtApptUpdateDS') }}