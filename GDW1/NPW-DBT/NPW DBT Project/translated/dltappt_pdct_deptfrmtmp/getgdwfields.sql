{{ config(materialized='view', tags=['DltAppt_Pdct_DeptFrmTMP']) }}

WITH GetGDWFields AS (
	SELECT
		{{ ref('Update__updates') }}.APPT_PDCT_I,
		{{ ref('Update__updates') }}.DEPT_ROLE_C,
		{{ ref('Update__updates') }}.SRCE_SYST_C,
		{{ ref('Update__updates') }}.EXPY_D,
		{{ ref('Update__updates') }}.PROS_KEY_EXPY_I,
		{{ ref('gdw_cpy') }}.EFFT_D
	FROM {{ ref('Update__updates') }}
	LEFT JOIN {{ ref('gdw_cpy') }} ON {{ ref('Update__updates') }}.APPT_PDCT_I = {{ ref('gdw_cpy') }}.APPT_PDCT_I
	AND {{ ref('Update__updates') }}.DEPT_ROLE_C = {{ ref('gdw_cpy') }}.DEPT_ROLE_C
	AND {{ ref('Update__updates') }}.SRCE_SYST_C = {{ ref('gdw_cpy') }}.SRCE_SYST_C
)

SELECT * FROM GetGDWFields