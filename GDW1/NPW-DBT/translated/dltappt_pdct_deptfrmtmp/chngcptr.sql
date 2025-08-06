{{ config(materialized='view', tags=['DltAppt_Pdct_DeptFrmTMP']) }}

WITH Chngcptr AS (
	SELECT
		COALESCE({{ ref('src_cpy') }}.APPT_PDCT_I, {{ ref('gdw_cpy') }}.APPT_PDCT_I) AS APPT_PDCT_I,
		COALESCE({{ ref('src_cpy') }}.DEPT_I, {{ ref('gdw_cpy') }}.DEPT_I) AS DEPT_I,
		COALESCE({{ ref('src_cpy') }}.DEPT_ROLE_C, {{ ref('gdw_cpy') }}.DEPT_ROLE_C) AS DEPT_ROLE_C,
		COALESCE({{ ref('src_cpy') }}.SRCE_SYST_C, {{ ref('gdw_cpy') }}.SRCE_SYST_C) AS SRCE_SYST_C,
		COALESCE({{ ref('src_cpy') }}.BRCH_N, {{ ref('gdw_cpy') }}.BRCH_N) AS BRCH_N,
		CASE
			WHEN {{ ref('gdw_cpy') }}.APPT_PDCT_I IS NULL AND {{ ref('gdw_cpy') }}.DEPT_ROLE_C IS NULL AND {{ ref('gdw_cpy') }}.SRCE_SYST_C IS NULL THEN '1'
			WHEN {{ ref('src_cpy') }}.APPT_PDCT_I IS NULL AND {{ ref('src_cpy') }}.DEPT_ROLE_C IS NULL AND {{ ref('src_cpy') }}.SRCE_SYST_C IS NULL THEN '2'
			WHEN ({{ ref('gdw_cpy') }}.APPT_PDCT_I = {{ ref('src_cpy') }}.APPT_PDCT_I AND {{ ref('gdw_cpy') }}.DEPT_ROLE_C = {{ ref('src_cpy') }}.DEPT_ROLE_C AND {{ ref('gdw_cpy') }}.SRCE_SYST_C = {{ ref('src_cpy') }}.SRCE_SYST_C) AND ({{ ref('gdw_cpy') }}.DEPT_I <> {{ ref('src_cpy') }}.DEPT_I OR {{ ref('gdw_cpy') }}.BRCH_N <> {{ ref('src_cpy') }}.BRCH_N) THEN '3'
			WHEN {{ ref('gdw_cpy') }}.APPT_PDCT_I = {{ ref('src_cpy') }}.APPT_PDCT_I AND {{ ref('gdw_cpy') }}.DEPT_ROLE_C = {{ ref('src_cpy') }}.DEPT_ROLE_C AND {{ ref('gdw_cpy') }}.SRCE_SYST_C = {{ ref('src_cpy') }}.SRCE_SYST_C AND {{ ref('gdw_cpy') }}.DEPT_I = {{ ref('src_cpy') }}.DEPT_I AND {{ ref('gdw_cpy') }}.BRCH_N = {{ ref('src_cpy') }}.BRCH_N THEN '0'
		END AS change_code 
	FROM {{ ref('gdw_cpy') }} 
	FULL OUTER JOIN {{ ref('src_cpy') }} 
	ON {{ ref('gdw_cpy') }}.APPT_PDCT_I = {{ ref('src_cpy') }}.APPT_PDCT_I
	AND {{ ref('gdw_cpy') }}.DEPT_ROLE_C = {{ ref('src_cpy') }}.DEPT_ROLE_C
	AND {{ ref('gdw_cpy') }}.SRCE_SYST_C = {{ ref('src_cpy') }}.SRCE_SYST_C
	WHERE change_code = '1' OR change_code = '3' OR change_code = '2'
)

SELECT * FROM Chngcptr