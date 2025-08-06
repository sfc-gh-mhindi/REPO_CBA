{{ config(materialized='view', tags=['DltAppt_Pdct_DeptFrmTMP']) }}

WITH Join AS (
	SELECT
		{{ ref('Chngcptr') }}.APPT_PDCT_I,
		{{ ref('Chngcptr') }}.DEPT_I,
		{{ ref('Chngcptr') }}.DEPT_ROLE_C,
		{{ ref('Chngcptr') }}.SRCE_SYST_C,
		{{ ref('Chngcptr') }}.BRCH_N,
		{{ ref('Chngcptr') }}.change_code,
		{{ ref('src_cpy') }}.EFFT_D,
		{{ ref('src_cpy') }}.EXPY_D,
		{{ ref('src_cpy') }}.PROS_KEY_EFFT_I,
		{{ ref('src_cpy') }}.PROS_KEY_EXPY_I,
		{{ ref('src_cpy') }}.RUN_STRM
	FROM {{ ref('Chngcptr') }}
	LEFT JOIN {{ ref('src_cpy') }} ON {{ ref('Chngcptr') }}.APPT_PDCT_I = {{ ref('src_cpy') }}.APPT_PDCT_I
	AND {{ ref('Chngcptr') }}.DEPT_ROLE_C = {{ ref('src_cpy') }}.DEPT_ROLE_C
	AND {{ ref('Chngcptr') }}.SRCE_SYST_C = {{ ref('src_cpy') }}.SRCE_SYST_C
)

SELECT * FROM Join