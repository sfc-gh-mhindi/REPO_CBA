{{ config(materialized='view', tags=['LdDltChl_App_Qstn']) }}

WITH CC_Identify_Deltas AS (
	SELECT
		COALESCE({{ ref('{{ ref('Copy') }}') }}.APPT_I, {{ ref('{{ ref('Copy') }}') }}.APPT_I) AS APPT_I,
		COALESCE({{ ref('{{ ref('Copy') }}') }}.QSTN_C, {{ ref('{{ ref('Copy') }}') }}.QSTN_C) AS QSTN_C,
		COALESCE({{ ref('{{ ref('Copy') }}') }}.RESP_C, {{ ref('{{ ref('Copy') }}') }}.RESP_C) AS RESP_C,
		COALESCE({{ ref('{{ ref('Copy') }}') }}.RESP_CMMT_X, {{ ref('{{ ref('Copy') }}') }}.RESP_CMMT_X) AS RESP_CMMT_X,
		COALESCE({{ ref('{{ ref('Copy') }}') }}.PATY_I, {{ ref('{{ ref('Copy') }}') }}.PATY_I) AS PATY_I,
		COALESCE({{ ref('{{ ref('Copy') }}') }}.ROW_SECU_ACCS_C, {{ ref('{{ ref('Copy') }}') }}.ROW_SECU_ACCS_C) AS ROW_SECU_ACCS_C,
		CASE
			WHEN {{ ref('{{ ref('Copy') }}') }}.APPT_I IS NULL AND {{ ref('{{ ref('Copy') }}') }}.QSTN_C IS NULL AND {{ ref('{{ ref('Copy') }}') }}.PATY_I IS NULL THEN '1'
			WHEN {{ ref('{{ ref('Copy') }}') }}.APPT_I IS NULL AND {{ ref('{{ ref('Copy') }}') }}.QSTN_C IS NULL AND {{ ref('{{ ref('Copy') }}') }}.PATY_I IS NULL THEN '2'
			WHEN ({{ ref('{{ ref('Copy') }}') }}.APPT_I = {{ ref('{{ ref('Copy') }}') }}.APPT_I AND {{ ref('{{ ref('Copy') }}') }}.QSTN_C = {{ ref('{{ ref('Copy') }}') }}.QSTN_C AND {{ ref('{{ ref('Copy') }}') }}.PATY_I = {{ ref('{{ ref('Copy') }}') }}.PATY_I) AND ({{ ref('{{ ref('Copy') }}') }}.RESP_C <> {{ ref('{{ ref('Copy') }}') }}.RESP_C OR {{ ref('{{ ref('Copy') }}') }}.RESP_CMMT_X <> {{ ref('{{ ref('Copy') }}') }}.RESP_CMMT_X) THEN '3'
			WHEN {{ ref('{{ ref('Copy') }}') }}.APPT_I = {{ ref('{{ ref('Copy') }}') }}.APPT_I AND {{ ref('{{ ref('Copy') }}') }}.QSTN_C = {{ ref('{{ ref('Copy') }}') }}.QSTN_C AND {{ ref('{{ ref('Copy') }}') }}.PATY_I = {{ ref('{{ ref('Copy') }}') }}.PATY_I AND {{ ref('{{ ref('Copy') }}') }}.RESP_C = {{ ref('{{ ref('Copy') }}') }}.RESP_C AND {{ ref('{{ ref('Copy') }}') }}.RESP_CMMT_X = {{ ref('{{ ref('Copy') }}') }}.RESP_CMMT_X THEN '0'
		END AS change_code 
	FROM {{ ref('{{ ref('Copy') }}') }} 
	FULL OUTER JOIN {{ ref('{{ ref('Copy') }}') }} 
	ON {{ ref('{{ ref('Copy') }}') }}.APPT_I = {{ ref('{{ ref('Copy') }}') }}.APPT_I
	AND {{ ref('{{ ref('Copy') }}') }}.QSTN_C = {{ ref('{{ ref('Copy') }}') }}.QSTN_C
	AND {{ ref('{{ ref('Copy') }}') }}.PATY_I = {{ ref('{{ ref('Copy') }}') }}.PATY_I
	WHERE change_code = '1' OR change_code = '3'
)

SELECT * FROM CC_Identify_Deltas