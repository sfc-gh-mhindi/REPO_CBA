{{ config(materialized='incremental', alias='appt_dept', incremental_strategy='append', tags=['LdApptDeptIns']) }}

SELECT
	APPT_I
	DEPT_ROLE_C
	EFFT_D
	DEPT_I
	EXPY_D
	PROS_KEY_EFFT_I
	PROS_KEY_EXPY_I
	EROR_SEQN_I 
FROM {{ ref('TgtApptDeptInsertDS') }}