{{ config(materialized='incremental', incremental_strategy='insert', tags=['LdApptOrigUpd']) }}

SELECT
	APPT_I
	APPT_ORIG_CATG_C
	EFFT_D
	EXPY_D
	PROS_KEY_EXPY_I 
FROM {{ ref('sq_ApptOrig_U') }}