{{ config(materialized='view', tags=['DltINT_GRUPFrmTMP_FA_UNDERTAKING']) }}

WITH ChangeCapture AS (
	SELECT
		COALESCE({{ ref('{{ ref('CpyFAUndertaking') }}') }}.INT_GRUP_I, {{ ref('{{ ref('CpyFAUndertaking') }}') }}.INT_GRUP_I) AS INT_GRUP_I,
		COALESCE({{ ref('{{ ref('CpyFAUndertaking') }}') }}.ORIG_SRCE_SYST_INT_GRUP_I, {{ ref('{{ ref('CpyFAUndertaking') }}') }}.ORIG_SRCE_SYST_INT_GRUP_I) AS ORIG_SRCE_SYST_INT_GRUP_I,
		COALESCE({{ ref('{{ ref('CpyFAUndertaking') }}') }}.INT_GRUP_M, {{ ref('{{ ref('CpyFAUndertaking') }}') }}.INT_GRUP_M) AS INT_GRUP_M,
		CASE
			WHEN {{ ref('{{ ref('CpyFAUndertaking') }}') }}.INT_GRUP_I IS NULL THEN '1'
			WHEN {{ ref('{{ ref('CpyFAUndertaking') }}') }}.INT_GRUP_I IS NULL THEN '2'
			WHEN ({{ ref('{{ ref('CpyFAUndertaking') }}') }}.INT_GRUP_I = {{ ref('{{ ref('CpyFAUndertaking') }}') }}.INT_GRUP_I) AND ({{ ref('{{ ref('CpyFAUndertaking') }}') }}.ORIG_SRCE_SYST_INT_GRUP_I <> {{ ref('{{ ref('CpyFAUndertaking') }}') }}.ORIG_SRCE_SYST_INT_GRUP_I OR {{ ref('{{ ref('CpyFAUndertaking') }}') }}.INT_GRUP_M <> {{ ref('{{ ref('CpyFAUndertaking') }}') }}.INT_GRUP_M) THEN '3'
			WHEN {{ ref('{{ ref('CpyFAUndertaking') }}') }}.INT_GRUP_I = {{ ref('{{ ref('CpyFAUndertaking') }}') }}.INT_GRUP_I AND {{ ref('{{ ref('CpyFAUndertaking') }}') }}.ORIG_SRCE_SYST_INT_GRUP_I = {{ ref('{{ ref('CpyFAUndertaking') }}') }}.ORIG_SRCE_SYST_INT_GRUP_I AND {{ ref('{{ ref('CpyFAUndertaking') }}') }}.INT_GRUP_M = {{ ref('{{ ref('CpyFAUndertaking') }}') }}.INT_GRUP_M THEN '0'
		END AS change_code 
	FROM {{ ref('{{ ref('CpyFAUndertaking') }}') }} 
	FULL OUTER JOIN {{ ref('{{ ref('CpyFAUndertaking') }}') }} 
	ON {{ ref('{{ ref('CpyFAUndertaking') }}') }}.INT_GRUP_I = {{ ref('{{ ref('CpyFAUndertaking') }}') }}.INT_GRUP_I
	WHERE change_code = '1' OR change_code = '3'
)

SELECT * FROM ChangeCapture