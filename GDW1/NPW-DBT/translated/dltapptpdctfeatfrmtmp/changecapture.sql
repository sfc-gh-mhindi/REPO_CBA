{{ config(materialized='view', tags=['DltApptPdctFeatFrmTMP']) }}

WITH ChangeCapture AS (
	SELECT
		COALESCE({{ ref('src_cpy') }}.APPT_PDCT_I, {{ ref('gdw_cpy') }}.APPT_PDCT_I) AS APPT_PDCT_I,
		COALESCE({{ ref('src_cpy') }}.FEAT_I, {{ ref('gdw_cpy') }}.FEAT_I) AS FEAT_I,
		COALESCE({{ ref('src_cpy') }}.SRCE_SYST_C, {{ ref('gdw_cpy') }}.SRCE_SYST_C) AS SRCE_SYST_C,
		COALESCE({{ ref('src_cpy') }}.ACTL_VALU_Q, {{ ref('gdw_cpy') }}.ACTL_VALU_Q) AS ACTL_VALU_Q,
		CASE
			WHEN {{ ref('gdw_cpy') }}.APPT_PDCT_I IS NULL AND {{ ref('gdw_cpy') }}.FEAT_I IS NULL AND {{ ref('gdw_cpy') }}.SRCE_SYST_C IS NULL THEN '1'
			WHEN {{ ref('src_cpy') }}.APPT_PDCT_I IS NULL AND {{ ref('src_cpy') }}.FEAT_I IS NULL AND {{ ref('src_cpy') }}.SRCE_SYST_C IS NULL THEN '2'
			WHEN ({{ ref('gdw_cpy') }}.APPT_PDCT_I = {{ ref('src_cpy') }}.APPT_PDCT_I AND {{ ref('gdw_cpy') }}.FEAT_I = {{ ref('src_cpy') }}.FEAT_I AND {{ ref('gdw_cpy') }}.SRCE_SYST_C = {{ ref('src_cpy') }}.SRCE_SYST_C) AND ({{ ref('gdw_cpy') }}.ACTL_VALU_Q <> {{ ref('src_cpy') }}.ACTL_VALU_Q) THEN '3'
			WHEN {{ ref('gdw_cpy') }}.APPT_PDCT_I = {{ ref('src_cpy') }}.APPT_PDCT_I AND {{ ref('gdw_cpy') }}.FEAT_I = {{ ref('src_cpy') }}.FEAT_I AND {{ ref('gdw_cpy') }}.SRCE_SYST_C = {{ ref('src_cpy') }}.SRCE_SYST_C AND {{ ref('gdw_cpy') }}.ACTL_VALU_Q = {{ ref('src_cpy') }}.ACTL_VALU_Q THEN '0'
		END AS change_code 
	FROM {{ ref('gdw_cpy') }} 
	FULL OUTER JOIN {{ ref('src_cpy') }} 
	ON {{ ref('gdw_cpy') }}.APPT_PDCT_I = {{ ref('src_cpy') }}.APPT_PDCT_I
	AND {{ ref('gdw_cpy') }}.FEAT_I = {{ ref('src_cpy') }}.FEAT_I
	AND {{ ref('gdw_cpy') }}.SRCE_SYST_C = {{ ref('src_cpy') }}.SRCE_SYST_C
	WHERE change_code = '1' OR change_code = '3' OR change_code = 'None' OR change_code = 'None'
)

SELECT * FROM ChangeCapture