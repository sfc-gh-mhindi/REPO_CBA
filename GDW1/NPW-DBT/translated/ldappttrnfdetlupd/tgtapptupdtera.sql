{{ config(materialized='incremental', alias='appt_trnf_detl', incremental_strategy='merge', tags=['LdApptTrnfDetlUpd']) }}

SELECT
	APPT_I
	APPT_TRNF_I
	EFFT_D
	EXPY_D
	PROS_KEY_EXPY_I 
FROM {{ ref('TgtApptUpdateDS') }}