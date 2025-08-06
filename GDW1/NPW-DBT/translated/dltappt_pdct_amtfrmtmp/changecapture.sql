{{ config(materialized='view', tags=['DltAppt_Pdct_AmtFrmTMP']) }}

WITH ChangeCapture AS (
	SELECT
		COALESCE({{ ref('src_cpy') }}.APPT_PDCT_I, {{ ref('gdw_cpy') }}.APPT_PDCT_I) AS APPT_PDCT_I,
		COALESCE({{ ref('src_cpy') }}.AMT_TYPE_C, {{ ref('gdw_cpy') }}.AMT_TYPE_C) AS AMT_TYPE_C,
		COALESCE({{ ref('src_cpy') }}.CNCY_C, {{ ref('gdw_cpy') }}.CNCY_C) AS CNCY_C,
		COALESCE({{ ref('src_cpy') }}.APPT_PDCT_A, {{ ref('gdw_cpy') }}.APPT_PDCT_A) AS APPT_PDCT_A,
		COALESCE({{ ref('src_cpy') }}.XCES_AMT_REAS_X, {{ ref('gdw_cpy') }}.XCES_AMT_REAS_X) AS XCES_AMT_REAS_X,
		COALESCE({{ ref('src_cpy') }}.SRCE_SYST_C, {{ ref('gdw_cpy') }}.SRCE_SYST_C) AS SRCE_SYST_C,
		CASE
			WHEN {{ ref('gdw_cpy') }}.APPT_PDCT_I IS NULL AND {{ ref('gdw_cpy') }}.AMT_TYPE_C IS NULL AND {{ ref('gdw_cpy') }}.SRCE_SYST_C IS NULL THEN '1'
			WHEN {{ ref('src_cpy') }}.APPT_PDCT_I IS NULL AND {{ ref('src_cpy') }}.AMT_TYPE_C IS NULL AND {{ ref('src_cpy') }}.SRCE_SYST_C IS NULL THEN '2'
			WHEN ({{ ref('gdw_cpy') }}.APPT_PDCT_I = {{ ref('src_cpy') }}.APPT_PDCT_I AND {{ ref('gdw_cpy') }}.AMT_TYPE_C = {{ ref('src_cpy') }}.AMT_TYPE_C AND {{ ref('gdw_cpy') }}.SRCE_SYST_C = {{ ref('src_cpy') }}.SRCE_SYST_C) AND ({{ ref('gdw_cpy') }}.CNCY_C <> {{ ref('src_cpy') }}.CNCY_C OR {{ ref('gdw_cpy') }}.APPT_PDCT_A <> {{ ref('src_cpy') }}.APPT_PDCT_A OR {{ ref('gdw_cpy') }}.XCES_AMT_REAS_X <> {{ ref('src_cpy') }}.XCES_AMT_REAS_X) THEN '3'
			WHEN {{ ref('gdw_cpy') }}.APPT_PDCT_I = {{ ref('src_cpy') }}.APPT_PDCT_I AND {{ ref('gdw_cpy') }}.AMT_TYPE_C = {{ ref('src_cpy') }}.AMT_TYPE_C AND {{ ref('gdw_cpy') }}.SRCE_SYST_C = {{ ref('src_cpy') }}.SRCE_SYST_C AND {{ ref('gdw_cpy') }}.CNCY_C = {{ ref('src_cpy') }}.CNCY_C AND {{ ref('gdw_cpy') }}.APPT_PDCT_A = {{ ref('src_cpy') }}.APPT_PDCT_A AND {{ ref('gdw_cpy') }}.XCES_AMT_REAS_X = {{ ref('src_cpy') }}.XCES_AMT_REAS_X THEN '0'
		END AS change_code 
	FROM {{ ref('gdw_cpy') }} 
	FULL OUTER JOIN {{ ref('src_cpy') }} 
	ON {{ ref('gdw_cpy') }}.APPT_PDCT_I = {{ ref('src_cpy') }}.APPT_PDCT_I
	AND {{ ref('gdw_cpy') }}.AMT_TYPE_C = {{ ref('src_cpy') }}.AMT_TYPE_C
	AND {{ ref('gdw_cpy') }}.SRCE_SYST_C = {{ ref('src_cpy') }}.SRCE_SYST_C
	WHERE change_code = '1' OR change_code = '3' OR change_code = '2'
)

SELECT * FROM ChangeCapture