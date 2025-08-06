{{ config(materialized='incremental', alias='appt_pdct_paty', incremental_strategy='append', tags=['LdApptPdctDeptInsert']) }}

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
FROM {{ ref('ApptPdctDeptInsert') }}