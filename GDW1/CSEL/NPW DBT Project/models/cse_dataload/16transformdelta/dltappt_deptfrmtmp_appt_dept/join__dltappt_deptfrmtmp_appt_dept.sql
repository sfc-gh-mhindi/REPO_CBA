WITH join_stage AS (
	SELECT
		a.NEW_APPT_I,
		a.NEW_DEPT_ROLE_C,
		b.NEW_DEPT_I,
		a.OLD_EFFT_D,
		b.change_code,
		a.OLD_DEPT_I
	FROM {{ ref('cpyapptdeptentseq__dltappt_deptfrmtmp_appt_dept') }} a
	INNER JOIN {{ ref('changecapture__dltappt_deptfrmtmp_appt_dept') }} b ON a.NEW_APPT_I = b.NEW_APPT_I
	AND a.NEW_DEPT_ROLE_C = b.NEW_DEPT_ROLE_C
)

SELECT 
	NEW_APPT_I,
	NEW_DEPT_ROLE_C,
	NEW_DEPT_I,
	OLD_EFFT_D,
	change_code,
	OLD_DEPT_I
FROM join_stage