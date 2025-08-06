{{ config(materialized='view', alias='evnt_dept', tags=['LdAPP_ANS_EVNT_DEPT_Upd']) }}

SELECT
	EVNT_I
	DEPT_ROLE_C
	EXPY_D
	PROS_KEY_EXPY_I 
FROM {{ ref('TgtAPPT_PDCT_DOCU_DELY_INSSUpDS') }}