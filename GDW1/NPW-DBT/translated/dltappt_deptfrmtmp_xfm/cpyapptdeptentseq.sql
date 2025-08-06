{{ config(materialized='view', tags=['DltAppt_DeptFrmTMP_XFM']) }}

WITH CpyApptDeptEntSeq AS (
	SELECT
		NEW_APPT_I,
		NEW_DEPT_ROLE_C,
		NEW_DEPT_I,
		{{ ref('SrcTmpApptDeptTera') }}.OLD_APPT_I AS NEW_APPT_I,
		{{ ref('SrcTmpApptDeptTera') }}.OLD_DEPT_ROLE_C AS NEW_DEPT_ROLE_C,
		{{ ref('SrcTmpApptDeptTera') }}.OLD_DEPT_I AS NEW_DEPT_I,
		OLD_DEPT_I,
		OLD_EFFT_D
	FROM {{ ref('SrcTmpApptDeptTera') }}
)

SELECT * FROM CpyApptDeptEntSeq