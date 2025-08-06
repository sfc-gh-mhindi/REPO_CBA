{{ config(materialized='view', tags=['DltPATY_INT_GRUPFrmTMP_FA_CLIENT_UNDERTAKING']) }}

WITH ChangeCapture AS (
	SELECT
		COALESCE({{ ref('{{ ref('CpyFAUndertaking') }}') }}.INT_GRUP_I, {{ ref('{{ ref('CpyFAUndertaking') }}') }}.INT_GRUP_I) AS INT_GRUP_I,
		COALESCE({{ ref('{{ ref('CpyFAUndertaking') }}') }}.REL_I, {{ ref('{{ ref('CpyFAUndertaking') }}') }}.REL_I) AS REL_I,
		COALESCE({{ ref('{{ ref('CpyFAUndertaking') }}') }}.SRCE_SYST_PATY_INT_GRUP_I, {{ ref('{{ ref('CpyFAUndertaking') }}') }}.SRCE_SYST_PATY_INT_GRUP_I) AS SRCE_SYST_PATY_INT_GRUP_I,
		COALESCE({{ ref('{{ ref('CpyFAUndertaking') }}') }}.ORIG_SRCE_SYST_PATY_I, {{ ref('{{ ref('CpyFAUndertaking') }}') }}.ORIG_SRCE_SYST_PATY_I) AS ORIG_SRCE_SYST_PATY_I,
		COALESCE({{ ref('{{ ref('CpyFAUndertaking') }}') }}.ORIG_SRCE_SYST_PATY_TYPE_C, {{ ref('{{ ref('CpyFAUndertaking') }}') }}.ORIG_SRCE_SYST_PATY_TYPE_C) AS ORIG_SRCE_SYST_PATY_TYPE_C,
		COALESCE({{ ref('{{ ref('CpyFAUndertaking') }}') }}.PRIM_CLNT_F, {{ ref('{{ ref('CpyFAUndertaking') }}') }}.PRIM_CLNT_F) AS PRIM_CLNT_F,
		COALESCE({{ ref('{{ ref('CpyFAUndertaking') }}') }}.PATY_I, {{ ref('{{ ref('CpyFAUndertaking') }}') }}.PATY_I) AS PATY_I,
		COALESCE({{ ref('{{ ref('CpyFAUndertaking') }}') }}.SRCE_SYST_C, {{ ref('{{ ref('CpyFAUndertaking') }}') }}.SRCE_SYST_C) AS SRCE_SYST_C,
		CASE
			WHEN {{ ref('{{ ref('CpyFAUndertaking') }}') }}.INT_GRUP_I IS NULL AND {{ ref('{{ ref('CpyFAUndertaking') }}') }}.SRCE_SYST_PATY_INT_GRUP_I IS NULL THEN '1'
			WHEN {{ ref('{{ ref('CpyFAUndertaking') }}') }}.INT_GRUP_I IS NULL AND {{ ref('{{ ref('CpyFAUndertaking') }}') }}.SRCE_SYST_PATY_INT_GRUP_I IS NULL THEN '2'
			WHEN ({{ ref('{{ ref('CpyFAUndertaking') }}') }}.INT_GRUP_I = {{ ref('{{ ref('CpyFAUndertaking') }}') }}.INT_GRUP_I AND {{ ref('{{ ref('CpyFAUndertaking') }}') }}.SRCE_SYST_PATY_INT_GRUP_I = {{ ref('{{ ref('CpyFAUndertaking') }}') }}.SRCE_SYST_PATY_INT_GRUP_I) AND ({{ ref('{{ ref('CpyFAUndertaking') }}') }}.REL_I <> {{ ref('{{ ref('CpyFAUndertaking') }}') }}.REL_I OR {{ ref('{{ ref('CpyFAUndertaking') }}') }}.ORIG_SRCE_SYST_PATY_I <> {{ ref('{{ ref('CpyFAUndertaking') }}') }}.ORIG_SRCE_SYST_PATY_I OR {{ ref('{{ ref('CpyFAUndertaking') }}') }}.ORIG_SRCE_SYST_PATY_TYPE_C <> {{ ref('{{ ref('CpyFAUndertaking') }}') }}.ORIG_SRCE_SYST_PATY_TYPE_C OR {{ ref('{{ ref('CpyFAUndertaking') }}') }}.PRIM_CLNT_F <> {{ ref('{{ ref('CpyFAUndertaking') }}') }}.PRIM_CLNT_F OR {{ ref('{{ ref('CpyFAUndertaking') }}') }}.PATY_I <> {{ ref('{{ ref('CpyFAUndertaking') }}') }}.PATY_I OR {{ ref('{{ ref('CpyFAUndertaking') }}') }}.SRCE_SYST_C <> {{ ref('{{ ref('CpyFAUndertaking') }}') }}.SRCE_SYST_C) THEN '3'
			WHEN {{ ref('{{ ref('CpyFAUndertaking') }}') }}.INT_GRUP_I = {{ ref('{{ ref('CpyFAUndertaking') }}') }}.INT_GRUP_I AND {{ ref('{{ ref('CpyFAUndertaking') }}') }}.SRCE_SYST_PATY_INT_GRUP_I = {{ ref('{{ ref('CpyFAUndertaking') }}') }}.SRCE_SYST_PATY_INT_GRUP_I AND {{ ref('{{ ref('CpyFAUndertaking') }}') }}.REL_I = {{ ref('{{ ref('CpyFAUndertaking') }}') }}.REL_I AND {{ ref('{{ ref('CpyFAUndertaking') }}') }}.ORIG_SRCE_SYST_PATY_I = {{ ref('{{ ref('CpyFAUndertaking') }}') }}.ORIG_SRCE_SYST_PATY_I AND {{ ref('{{ ref('CpyFAUndertaking') }}') }}.ORIG_SRCE_SYST_PATY_TYPE_C = {{ ref('{{ ref('CpyFAUndertaking') }}') }}.ORIG_SRCE_SYST_PATY_TYPE_C AND {{ ref('{{ ref('CpyFAUndertaking') }}') }}.PRIM_CLNT_F = {{ ref('{{ ref('CpyFAUndertaking') }}') }}.PRIM_CLNT_F AND {{ ref('{{ ref('CpyFAUndertaking') }}') }}.PATY_I = {{ ref('{{ ref('CpyFAUndertaking') }}') }}.PATY_I AND {{ ref('{{ ref('CpyFAUndertaking') }}') }}.SRCE_SYST_C = {{ ref('{{ ref('CpyFAUndertaking') }}') }}.SRCE_SYST_C THEN '0'
		END AS change_code 
	FROM {{ ref('{{ ref('CpyFAUndertaking') }}') }} 
	FULL OUTER JOIN {{ ref('{{ ref('CpyFAUndertaking') }}') }} 
	ON {{ ref('{{ ref('CpyFAUndertaking') }}') }}.INT_GRUP_I = {{ ref('{{ ref('CpyFAUndertaking') }}') }}.INT_GRUP_I
	AND {{ ref('{{ ref('CpyFAUndertaking') }}') }}.SRCE_SYST_PATY_INT_GRUP_I = {{ ref('{{ ref('CpyFAUndertaking') }}') }}.SRCE_SYST_PATY_INT_GRUP_I
	WHERE change_code = '1' OR change_code = '3'
)

SELECT * FROM ChangeCapture