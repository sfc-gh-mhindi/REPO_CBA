{{ config(materialized='incremental', alias='tmp_appt_empl', incremental_strategy='append', tags=['LdTMP_APPT_EMPLrmXfm']) }}

SELECT
	EMPL_I
	APPT_I
	EMPL_ROLE_C
	EFFT_D
	EXPY_D
	PROS_KEY_EFFT_I
	PROS_KEY_EXPY_I
	EROR_SEQN_I
	RUN_STRM 
FROM {{ ref('SrcApptEmplDS') }}