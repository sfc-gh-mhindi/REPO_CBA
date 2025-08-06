{{ config(materialized='incremental', alias='tmp_appt_stus', incremental_strategy='append', tags=['LdTmp_Appt_StusFrmXfmr']) }}

SELECT
	APPT_I
	STUS_C
	STRT_S
	STRT_D
	STRT_T
	END_D
	END_T
	END_S
	EMPL_I
	EFFT_D
	EXPY_D
	PROS_KEY_EFFT_I
	PROS_KEY_EXPY_I
	EROR_SEQN_I
	RUN_STRM 
FROM {{ ref('TgtAppt_Stus') }}