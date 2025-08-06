{{ config(materialized='incremental', alias='tmp_appt_pdct_dept', incremental_strategy='append', tags=['LdTmp_Appt_Pdct_DeptFrmXfm']) }}

SELECT
	APPT_PDCT_I
	DEPT_I
	DEPT_ROLE_C
	SRCE_SYST_C
	BRCH_N
	EFFT_D
	EXPY_D
	PROS_KEY_EFFT_I
	PROS_KEY_EXPY_I
	RUN_STRM 
FROM {{ ref('TgtAppt_Pdct') }}