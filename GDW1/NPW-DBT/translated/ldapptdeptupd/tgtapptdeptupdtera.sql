{{ config(materialized='incremental', alias='appt_dept', incremental_strategy='merge', tags=['LdApptDeptUpd']) }}

SELECT
	DEPT_I
	APPT_I
	DEPT_ROLE_C
	EFFT_D
	EXPY_D
	PROS_KEY_EXPY_I 
FROM {{ ref('TgtApptDeptUpdateDS') }}