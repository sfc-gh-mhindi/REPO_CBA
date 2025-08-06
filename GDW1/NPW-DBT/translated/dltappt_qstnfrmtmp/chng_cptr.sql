{{ config(materialized='view', tags=['DltAppt_QstnFrmTMP']) }}

WITH Chng_Cptr AS (
	SELECT
		COALESCE({{ ref('src_cpy') }}.APPT_I, {{ ref('gdw_cpy') }}.APPT_I) AS APPT_I,
		COALESCE({{ ref('src_cpy') }}.QSTN_C, {{ ref('gdw_cpy') }}.QSTN_C) AS QSTN_C,
		COALESCE({{ ref('src_cpy') }}.RESP_C, {{ ref('gdw_cpy') }}.RESP_C) AS RESP_C,
		COALESCE({{ ref('src_cpy') }}.RESP_CMMT_X, {{ ref('gdw_cpy') }}.RESP_CMMT_X) AS RESP_CMMT_X,
		COALESCE({{ ref('src_cpy') }}.PATY_I, {{ ref('gdw_cpy') }}.PATY_I) AS PATY_I,
		CASE
			WHEN {{ ref('gdw_cpy') }}.APPT_I IS NULL AND {{ ref('gdw_cpy') }}.QSTN_C IS NULL THEN '1'
			WHEN {{ ref('src_cpy') }}.APPT_I IS NULL AND {{ ref('src_cpy') }}.QSTN_C IS NULL THEN '2'
			WHEN ({{ ref('gdw_cpy') }}.APPT_I = {{ ref('src_cpy') }}.APPT_I AND {{ ref('gdw_cpy') }}.QSTN_C = {{ ref('src_cpy') }}.QSTN_C) AND ({{ ref('gdw_cpy') }}.PATY_I <> {{ ref('src_cpy') }}.PATY_I OR {{ ref('gdw_cpy') }}.RESP_C <> {{ ref('src_cpy') }}.RESP_C OR {{ ref('gdw_cpy') }}.RESP_CMMT_X <> {{ ref('src_cpy') }}.RESP_CMMT_X) THEN '3'
			WHEN {{ ref('gdw_cpy') }}.APPT_I = {{ ref('src_cpy') }}.APPT_I AND {{ ref('gdw_cpy') }}.QSTN_C = {{ ref('src_cpy') }}.QSTN_C AND {{ ref('gdw_cpy') }}.PATY_I = {{ ref('src_cpy') }}.PATY_I AND {{ ref('gdw_cpy') }}.RESP_C = {{ ref('src_cpy') }}.RESP_C AND {{ ref('gdw_cpy') }}.RESP_CMMT_X = {{ ref('src_cpy') }}.RESP_CMMT_X THEN '0'
		END AS change_code 
	FROM {{ ref('gdw_cpy') }} 
	FULL OUTER JOIN {{ ref('src_cpy') }} 
	ON {{ ref('gdw_cpy') }}.APPT_I = {{ ref('src_cpy') }}.APPT_I
	AND {{ ref('gdw_cpy') }}.QSTN_C = {{ ref('src_cpy') }}.QSTN_C
	WHERE change_code = '1' OR change_code = '3' OR change_code = '2'
)

SELECT * FROM Chng_Cptr