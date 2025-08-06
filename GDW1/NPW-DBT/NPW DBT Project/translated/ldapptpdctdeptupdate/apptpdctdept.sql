{{ config(materialized='incremental', alias='appt_pdct_dept', incremental_strategy='merge', tags=['LdApptPdctDeptUpdate']) }}

SELECT
	APPT_PDCT_I
	DEPT_ROLE_C
	SRCE_SYST_C
	EFFT_D
	EXPY_D
	PROS_KEY_EXPY_I 
FROM {{ ref('ApptPdctDeptUpdate') }}