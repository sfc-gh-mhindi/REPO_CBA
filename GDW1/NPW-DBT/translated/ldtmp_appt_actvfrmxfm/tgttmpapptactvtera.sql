{{ config(materialized='incremental', alias='tmp_appt_actv', incremental_strategy='append', tags=['LdTMP_APPT_ACTVFrmXfm']) }}

SELECT
	APPT_I
	APPT_ACTV_Q
	RUN_STRM 
FROM {{ ref('Cpy') }}