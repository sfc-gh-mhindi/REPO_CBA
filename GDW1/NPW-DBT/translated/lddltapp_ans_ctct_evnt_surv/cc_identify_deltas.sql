{{ config(materialized='view', tags=['LdDltAPP_ANS_CTCT_EVNT_SURV']) }}

WITH CC_Identify_Deltas AS (
	SELECT
		COALESCE({{ ref('{{ ref('Copy') }}') }}.EVNT_I, {{ ref('{{ ref('Copy') }}') }}.EVNT_I) AS EVNT_I,
		COALESCE({{ ref('{{ ref('Copy') }}') }}.QSTN_C, {{ ref('{{ ref('Copy') }}') }}.QSTN_C) AS QSTN_C,
		COALESCE({{ ref('{{ ref('Copy') }}') }}.RESP_C, {{ ref('{{ ref('Copy') }}') }}.RESP_C) AS RESP_C,
		COALESCE({{ ref('{{ ref('Copy') }}') }}.RESP_CMMT_X, {{ ref('{{ ref('Copy') }}') }}.RESP_CMMT_X) AS RESP_CMMT_X,
		CASE
			WHEN {{ ref('{{ ref('Copy') }}') }}.EVNT_I IS NULL AND {{ ref('{{ ref('Copy') }}') }}.QSTN_C IS NULL THEN '1'
			WHEN {{ ref('{{ ref('Copy') }}') }}.EVNT_I IS NULL AND {{ ref('{{ ref('Copy') }}') }}.QSTN_C IS NULL THEN '2'
			WHEN ({{ ref('{{ ref('Copy') }}') }}.EVNT_I = {{ ref('{{ ref('Copy') }}') }}.EVNT_I AND {{ ref('{{ ref('Copy') }}') }}.QSTN_C = {{ ref('{{ ref('Copy') }}') }}.QSTN_C) AND ({{ ref('{{ ref('Copy') }}') }}.RESP_C <> {{ ref('{{ ref('Copy') }}') }}.RESP_C OR {{ ref('{{ ref('Copy') }}') }}.RESP_CMMT_X <> {{ ref('{{ ref('Copy') }}') }}.RESP_CMMT_X) THEN '3'
			WHEN {{ ref('{{ ref('Copy') }}') }}.EVNT_I = {{ ref('{{ ref('Copy') }}') }}.EVNT_I AND {{ ref('{{ ref('Copy') }}') }}.QSTN_C = {{ ref('{{ ref('Copy') }}') }}.QSTN_C AND {{ ref('{{ ref('Copy') }}') }}.RESP_C = {{ ref('{{ ref('Copy') }}') }}.RESP_C AND {{ ref('{{ ref('Copy') }}') }}.RESP_CMMT_X = {{ ref('{{ ref('Copy') }}') }}.RESP_CMMT_X THEN '0'
		END AS change_code 
	FROM {{ ref('{{ ref('Copy') }}') }} 
	FULL OUTER JOIN {{ ref('{{ ref('Copy') }}') }} 
	ON {{ ref('{{ ref('Copy') }}') }}.EVNT_I = {{ ref('{{ ref('Copy') }}') }}.EVNT_I
	AND {{ ref('{{ ref('Copy') }}') }}.QSTN_C = {{ ref('{{ ref('Copy') }}') }}.QSTN_C
	WHERE change_code = '1' OR change_code = '3'
)

SELECT * FROM CC_Identify_Deltas