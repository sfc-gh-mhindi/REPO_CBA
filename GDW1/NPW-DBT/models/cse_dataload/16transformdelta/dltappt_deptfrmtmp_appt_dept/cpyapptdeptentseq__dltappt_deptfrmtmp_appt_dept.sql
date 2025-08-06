WITH CpyApptDeptEntSeq AS (
	SELECT
		NEW_APPT_I,
		NEW_DEPT_ROLE_C,
		NEW_DEPT_I,
		OLD_APPT_I,
		OLD_DEPT_ROLE_C,
		OLD_DEPT_I,
		OLD_EFFT_D
	FROM {{ ref('srctmpapptdepttera__dltappt_deptfrmtmp_appt_dept') }}
)

SELECT * FROM CpyApptDeptEntSeq