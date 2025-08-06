{{ config(materialized='view', alias='appt_trnf_detl', tags=['LdDelFlagAPPT_TRNF_DETLUpd']) }}

SELECT
	APPT_I
	APPT_TRNF_I
	EXPY_D
	PROS_KEY_EXPY_I 
FROM {{ ref('ApptTrnfDetlDS') }}