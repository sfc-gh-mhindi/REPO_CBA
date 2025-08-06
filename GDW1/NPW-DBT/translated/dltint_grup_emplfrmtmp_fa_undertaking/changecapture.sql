{{ config(materialized='view', tags=['DltINT_GRUP_EMPLFrmTMP_FA_UNDERTAKING']) }}

WITH ChangeCapture AS (
	SELECT
		COALESCE({{ ref('{{ ref('CpyFAUtak') }}') }}.INT_GRUP_I, {{ ref('{{ ref('CpyFAUtak') }}') }}.INT_GRUP_I) AS INT_GRUP_I,
		CASE
			WHEN {{ ref('{{ ref('CpyFAUtak') }}') }}.INT_GRUP_I IS NULL THEN '1'
			WHEN {{ ref('{{ ref('CpyFAUtak') }}') }}.INT_GRUP_I IS NULL THEN '2'
			WHEN ({{ ref('{{ ref('CpyFAUtak') }}') }}.INT_GRUP_I = {{ ref('{{ ref('CpyFAUtak') }}') }}.INT_GRUP_I) AND (NULL) THEN '3'
			WHEN {{ ref('{{ ref('CpyFAUtak') }}') }}.INT_GRUP_I = {{ ref('{{ ref('CpyFAUtak') }}') }}.INT_GRUP_I THEN '0'
		END AS change_code 
	FROM {{ ref('{{ ref('CpyFAUtak') }}') }} 
	FULL OUTER JOIN {{ ref('{{ ref('CpyFAUtak') }}') }} 
	ON {{ ref('{{ ref('CpyFAUtak') }}') }}.INT_GRUP_I = {{ ref('{{ ref('CpyFAUtak') }}') }}.INT_GRUP_I
	WHERE change_code = '1'
)

SELECT * FROM ChangeCapture