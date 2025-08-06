{{ config(materialized='view', tags=['DltAppt_DeptFrmTMP_XFM']) }}

WITH Join AS (
	SELECT
		{{ ref('CpyApptDeptEntSeq') }}.NEW_APPT_I,
		{{ ref('CpyApptDeptEntSeq') }}.NEW_DEPT_ROLE_C,
		{{ ref('ChangeCapture') }}.NEW_DEPT_I,
		{{ ref('CpyApptDeptEntSeq') }}.OLD_EFFT_D,
		{{ ref('ChangeCapture') }}.change_code,
		{{ ref('CpyApptDeptEntSeq') }}.OLD_DEPT_I
	FROM {{ ref('CpyApptDeptEntSeq') }}
	INNER JOIN {{ ref('ChangeCapture') }} ON {{ ref('CpyApptDeptEntSeq') }}.NEW_APPT_I = {{ ref('ChangeCapture') }}.NEW_APPT_I
	AND {{ ref('CpyApptDeptEntSeq') }}.NEW_DEPT_ROLE_C = {{ ref('ChangeCapture') }}.NEW_DEPT_ROLE_C
)

SELECT * FROM Join