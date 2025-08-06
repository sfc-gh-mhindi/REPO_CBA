{{ config(materialized='view', tags=['LdDltAPP_ANS_EVNT_DEPT']) }}

WITH CC_Identify_Deltas AS (
	SELECT
		COALESCE({{ ref('{{ ref('Copy') }}') }}.EVNT_I, {{ ref('{{ ref('Copy') }}') }}.EVNT_I) AS EVNT_I,
		COALESCE({{ ref('{{ ref('Copy') }}') }}.DEPT_ROLE_C, {{ ref('{{ ref('Copy') }}') }}.DEPT_ROLE_C) AS DEPT_ROLE_C,
		COALESCE({{ ref('{{ ref('Copy') }}') }}.DEPT_I, {{ ref('{{ ref('Copy') }}') }}.DEPT_I) AS DEPT_I,
		CASE
			WHEN {{ ref('{{ ref('Copy') }}') }}.EVNT_I IS NULL AND {{ ref('{{ ref('Copy') }}') }}.DEPT_ROLE_C IS NULL THEN '1'
			WHEN {{ ref('{{ ref('Copy') }}') }}.EVNT_I IS NULL AND {{ ref('{{ ref('Copy') }}') }}.DEPT_ROLE_C IS NULL THEN '2'
			WHEN ({{ ref('{{ ref('Copy') }}') }}.EVNT_I = {{ ref('{{ ref('Copy') }}') }}.EVNT_I AND {{ ref('{{ ref('Copy') }}') }}.DEPT_ROLE_C = {{ ref('{{ ref('Copy') }}') }}.DEPT_ROLE_C) AND ({{ ref('{{ ref('Copy') }}') }}.DEPT_I <> {{ ref('{{ ref('Copy') }}') }}.DEPT_I) THEN '3'
			WHEN {{ ref('{{ ref('Copy') }}') }}.EVNT_I = {{ ref('{{ ref('Copy') }}') }}.EVNT_I AND {{ ref('{{ ref('Copy') }}') }}.DEPT_ROLE_C = {{ ref('{{ ref('Copy') }}') }}.DEPT_ROLE_C AND {{ ref('{{ ref('Copy') }}') }}.DEPT_I = {{ ref('{{ ref('Copy') }}') }}.DEPT_I THEN '0'
		END AS change_code 
	FROM {{ ref('{{ ref('Copy') }}') }} 
	FULL OUTER JOIN {{ ref('{{ ref('Copy') }}') }} 
	ON {{ ref('{{ ref('Copy') }}') }}.EVNT_I = {{ ref('{{ ref('Copy') }}') }}.EVNT_I
	AND {{ ref('{{ ref('Copy') }}') }}.DEPT_ROLE_C = {{ ref('{{ ref('Copy') }}') }}.DEPT_ROLE_C
	WHERE change_code = '1' OR change_code = '3'
)

SELECT * FROM CC_Identify_Deltas