{{ config(materialized='view', tags=['DltAppt_Pdct_RpayFrmTMP']) }}

WITH ChangeCapture AS (
	SELECT
		COALESCE({{ ref('src_cpy') }}.APPT_PDCT_I, {{ ref('gdw_cpy') }}.APPT_PDCT_I) AS APPT_PDCT_I,
		COALESCE({{ ref('src_cpy') }}.SRCE_SYST_C, {{ ref('gdw_cpy') }}.SRCE_SYST_C) AS SRCE_SYST_C,
		COALESCE({{ ref('src_cpy') }}.RPAY_SRCE_C, {{ ref('gdw_cpy') }}.RPAY_SRCE_C) AS RPAY_SRCE_C,
		COALESCE({{ ref('src_cpy') }}.RPAY_SRCE_OTHR_X, {{ ref('gdw_cpy') }}.RPAY_SRCE_OTHR_X) AS RPAY_SRCE_OTHR_X,
		CASE
			WHEN {{ ref('gdw_cpy') }}.APPT_PDCT_I IS NULL AND {{ ref('gdw_cpy') }}.SRCE_SYST_C IS NULL THEN '1'
			WHEN {{ ref('src_cpy') }}.APPT_PDCT_I IS NULL AND {{ ref('src_cpy') }}.SRCE_SYST_C IS NULL THEN '2'
			WHEN ({{ ref('gdw_cpy') }}.APPT_PDCT_I = {{ ref('src_cpy') }}.APPT_PDCT_I AND {{ ref('gdw_cpy') }}.SRCE_SYST_C = {{ ref('src_cpy') }}.SRCE_SYST_C) AND ({{ ref('gdw_cpy') }}.RPAY_SRCE_C <> {{ ref('src_cpy') }}.RPAY_SRCE_C OR {{ ref('gdw_cpy') }}.RPAY_SRCE_OTHR_X <> {{ ref('src_cpy') }}.RPAY_SRCE_OTHR_X) THEN '3'
			WHEN {{ ref('gdw_cpy') }}.APPT_PDCT_I = {{ ref('src_cpy') }}.APPT_PDCT_I AND {{ ref('gdw_cpy') }}.SRCE_SYST_C = {{ ref('src_cpy') }}.SRCE_SYST_C AND {{ ref('gdw_cpy') }}.RPAY_SRCE_C = {{ ref('src_cpy') }}.RPAY_SRCE_C AND {{ ref('gdw_cpy') }}.RPAY_SRCE_OTHR_X = {{ ref('src_cpy') }}.RPAY_SRCE_OTHR_X THEN '0'
		END AS change_code 
	FROM {{ ref('gdw_cpy') }} 
	FULL OUTER JOIN {{ ref('src_cpy') }} 
	ON {{ ref('gdw_cpy') }}.APPT_PDCT_I = {{ ref('src_cpy') }}.APPT_PDCT_I
	AND {{ ref('gdw_cpy') }}.SRCE_SYST_C = {{ ref('src_cpy') }}.SRCE_SYST_C
	WHERE change_code = '1' OR change_code = '3'
)

SELECT * FROM ChangeCapture