{{ config(materialized='incremental', alias='evnt_dept', incremental_strategy='append', tags=['LdAPP_ANS_EVNT_DEPT_Ins']) }}

SELECT
	EVNT_I
	DEPT_ROLE_C
	EFFT_D
	DEPT_I
	DEPT_RPRT_I
	TEAM_I
	EXPY_D
	PROS_KEY_EFFT_I
	PROS_KEY_EXPY_I
	EROR_SEQN_I
	ROW_SECU_ACCS_C 
FROM {{ ref('TgtInsertDS') }}