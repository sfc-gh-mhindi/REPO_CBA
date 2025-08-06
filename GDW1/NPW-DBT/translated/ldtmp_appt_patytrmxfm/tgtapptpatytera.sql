{{ config(materialized='incremental', alias='tmp_appt_paty', incremental_strategy='append', tags=['LdTMP_APPT_PATYTrmXfm']) }}

SELECT
	APPT_I
	PATY_I
	REL_C
	REL_REAS_C
	REL_STUS_C
	REL_LEVL_C
	SRCE_SYST_C
	RUN_STRM 
FROM {{ ref('TgtCclAppPremapDS') }}