{{ config(materialized='view', tags=['DltAppt_StusFrmTMP']) }}

WITH ChangeCapture AS (
	SELECT
		COALESCE({{ ref('src_cpy') }}.APPT_I, {{ ref('gdw_cpy') }}.APPT_I) AS APPT_I,
		COALESCE({{ ref('src_cpy') }}.STUS_C, {{ ref('gdw_cpy') }}.STUS_C) AS STUS_C,
		COALESCE({{ ref('src_cpy') }}.STRT_S, {{ ref('gdw_cpy') }}.STRT_S) AS STRT_S,
		COALESCE({{ ref('src_cpy') }}.STRT_D, {{ ref('gdw_cpy') }}.STRT_D) AS STRT_D,
		COALESCE({{ ref('src_cpy') }}.STRT_T, {{ ref('gdw_cpy') }}.STRT_T) AS STRT_T,
		COALESCE({{ ref('src_cpy') }}.END_D, {{ ref('gdw_cpy') }}.END_D) AS END_D,
		COALESCE({{ ref('src_cpy') }}.END_T, {{ ref('gdw_cpy') }}.END_T) AS END_T,
		COALESCE({{ ref('src_cpy') }}.END_S, {{ ref('gdw_cpy') }}.END_S) AS END_S,
		COALESCE({{ ref('src_cpy') }}.EMPL_I, {{ ref('gdw_cpy') }}.EMPL_I) AS EMPL_I,
		CASE
			WHEN {{ ref('gdw_cpy') }}.APPT_I IS NULL THEN '1'
			WHEN {{ ref('src_cpy') }}.APPT_I IS NULL THEN '2'
			WHEN ({{ ref('gdw_cpy') }}.APPT_I = {{ ref('src_cpy') }}.APPT_I) AND ({{ ref('gdw_cpy') }}.END_D <> {{ ref('src_cpy') }}.END_D OR {{ ref('gdw_cpy') }}.END_S <> {{ ref('src_cpy') }}.END_S OR {{ ref('gdw_cpy') }}.END_T <> {{ ref('src_cpy') }}.END_T OR {{ ref('gdw_cpy') }}.STRT_D <> {{ ref('src_cpy') }}.STRT_D OR {{ ref('gdw_cpy') }}.STRT_S <> {{ ref('src_cpy') }}.STRT_S OR {{ ref('gdw_cpy') }}.STRT_T <> {{ ref('src_cpy') }}.STRT_T OR {{ ref('gdw_cpy') }}.STUS_C <> {{ ref('src_cpy') }}.STUS_C OR {{ ref('gdw_cpy') }}.EMPL_I <> {{ ref('src_cpy') }}.EMPL_I) THEN '3'
			WHEN {{ ref('gdw_cpy') }}.APPT_I = {{ ref('src_cpy') }}.APPT_I AND {{ ref('gdw_cpy') }}.END_D = {{ ref('src_cpy') }}.END_D AND {{ ref('gdw_cpy') }}.END_S = {{ ref('src_cpy') }}.END_S AND {{ ref('gdw_cpy') }}.END_T = {{ ref('src_cpy') }}.END_T AND {{ ref('gdw_cpy') }}.STRT_D = {{ ref('src_cpy') }}.STRT_D AND {{ ref('gdw_cpy') }}.STRT_S = {{ ref('src_cpy') }}.STRT_S AND {{ ref('gdw_cpy') }}.STRT_T = {{ ref('src_cpy') }}.STRT_T AND {{ ref('gdw_cpy') }}.STUS_C = {{ ref('src_cpy') }}.STUS_C AND {{ ref('gdw_cpy') }}.EMPL_I = {{ ref('src_cpy') }}.EMPL_I THEN '0'
		END AS change_code 
	FROM {{ ref('gdw_cpy') }} 
	FULL OUTER JOIN {{ ref('src_cpy') }} 
	ON {{ ref('gdw_cpy') }}.APPT_I = {{ ref('src_cpy') }}.APPT_I
	WHERE change_code = '1' OR change_code = '3' OR change_code = '2'
)

SELECT * FROM ChangeCapture