{{ config(materialized='view', tags=['LdDltAPP_ANS_EVNT_EMPL']) }}

WITH CC_Identify_Deltas AS (
	SELECT
		COALESCE({{ ref('{{ ref('Copy') }}') }}.EVNT_I, {{ ref('{{ ref('Copy') }}') }}.EVNT_I) AS EVNT_I,
		COALESCE({{ ref('{{ ref('Copy') }}') }}.EVNT_PATY_ROLE_TYPE_C, {{ ref('{{ ref('Copy') }}') }}.EVNT_PATY_ROLE_TYPE_C) AS EVNT_PATY_ROLE_TYPE_C,
		COALESCE({{ ref('{{ ref('Copy') }}') }}.EMPL_I, {{ ref('{{ ref('Copy') }}') }}.EMPL_I) AS EMPL_I,
		CASE
			WHEN {{ ref('{{ ref('Copy') }}') }}.EVNT_I IS NULL AND {{ ref('{{ ref('Copy') }}') }}.EVNT_PATY_ROLE_TYPE_C IS NULL THEN '1'
			WHEN {{ ref('{{ ref('Copy') }}') }}.EVNT_I IS NULL AND {{ ref('{{ ref('Copy') }}') }}.EVNT_PATY_ROLE_TYPE_C IS NULL THEN '2'
			WHEN ({{ ref('{{ ref('Copy') }}') }}.EVNT_I = {{ ref('{{ ref('Copy') }}') }}.EVNT_I AND {{ ref('{{ ref('Copy') }}') }}.EVNT_PATY_ROLE_TYPE_C = {{ ref('{{ ref('Copy') }}') }}.EVNT_PATY_ROLE_TYPE_C) AND ({{ ref('{{ ref('Copy') }}') }}.EMPL_I <> {{ ref('{{ ref('Copy') }}') }}.EMPL_I) THEN '3'
			WHEN {{ ref('{{ ref('Copy') }}') }}.EVNT_I = {{ ref('{{ ref('Copy') }}') }}.EVNT_I AND {{ ref('{{ ref('Copy') }}') }}.EVNT_PATY_ROLE_TYPE_C = {{ ref('{{ ref('Copy') }}') }}.EVNT_PATY_ROLE_TYPE_C AND {{ ref('{{ ref('Copy') }}') }}.EMPL_I = {{ ref('{{ ref('Copy') }}') }}.EMPL_I THEN '0'
		END AS change_code 
	FROM {{ ref('{{ ref('Copy') }}') }} 
	FULL OUTER JOIN {{ ref('{{ ref('Copy') }}') }} 
	ON {{ ref('{{ ref('Copy') }}') }}.EVNT_I = {{ ref('{{ ref('Copy') }}') }}.EVNT_I
	AND {{ ref('{{ ref('Copy') }}') }}.EVNT_PATY_ROLE_TYPE_C = {{ ref('{{ ref('Copy') }}') }}.EVNT_PATY_ROLE_TYPE_C
	WHERE change_code = '1' OR change_code = '3'
)

SELECT * FROM CC_Identify_Deltas