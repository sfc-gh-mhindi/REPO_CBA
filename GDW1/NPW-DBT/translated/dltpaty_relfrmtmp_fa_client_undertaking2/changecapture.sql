{{ config(materialized='view', tags=['DltPATY_RELFrmTMP_FA_CLIENT_UNDERTAKING2']) }}

WITH ChangeCapture AS (
	SELECT
		COALESCE({{ ref('{{ ref('CpyFAClientUndertaking') }}') }}.REL_I, {{ ref('{{ ref('CpyFAClientUndertaking') }}') }}.REL_I) AS REL_I,
		COALESCE({{ ref('{{ ref('CpyFAClientUndertaking') }}') }}.SRCE_SYST_REL_I, {{ ref('{{ ref('CpyFAClientUndertaking') }}') }}.SRCE_SYST_REL_I) AS SRCE_SYST_REL_I,
		COALESCE({{ ref('{{ ref('CpyFAClientUndertaking') }}') }}.REL_TYPE_C, {{ ref('{{ ref('CpyFAClientUndertaking') }}') }}.REL_TYPE_C) AS REL_TYPE_C,
		COALESCE({{ ref('{{ ref('CpyFAClientUndertaking') }}') }}.RELD_PATY_I, {{ ref('{{ ref('CpyFAClientUndertaking') }}') }}.RELD_PATY_I) AS RELD_PATY_I,
		COALESCE({{ ref('{{ ref('CpyFAClientUndertaking') }}') }}.SRCE_SYST_C, {{ ref('{{ ref('CpyFAClientUndertaking') }}') }}.SRCE_SYST_C) AS SRCE_SYST_C,
		COALESCE({{ ref('{{ ref('CpyFAClientUndertaking') }}') }}.PATY_ROLE_C, {{ ref('{{ ref('CpyFAClientUndertaking') }}') }}.PATY_ROLE_C) AS PATY_ROLE_C,
		COALESCE({{ ref('{{ ref('CpyFAClientUndertaking') }}') }}.REL_STUS_C, {{ ref('{{ ref('CpyFAClientUndertaking') }}') }}.REL_STUS_C) AS REL_STUS_C,
		COALESCE({{ ref('{{ ref('CpyFAClientUndertaking') }}') }}.REL_EFFT_D, {{ ref('{{ ref('CpyFAClientUndertaking') }}') }}.REL_EFFT_D) AS REL_EFFT_D,
		COALESCE({{ ref('{{ ref('CpyFAClientUndertaking') }}') }}.REL_EXPY_D, {{ ref('{{ ref('CpyFAClientUndertaking') }}') }}.REL_EXPY_D) AS REL_EXPY_D,
		COALESCE({{ ref('{{ ref('CpyFAClientUndertaking') }}') }}.REL_REAS_C, {{ ref('{{ ref('CpyFAClientUndertaking') }}') }}.REL_REAS_C) AS REL_REAS_C,
		COALESCE({{ ref('{{ ref('CpyFAClientUndertaking') }}') }}.REL_LEVL_C, {{ ref('{{ ref('CpyFAClientUndertaking') }}') }}.REL_LEVL_C) AS REL_LEVL_C,
		COALESCE({{ ref('{{ ref('CpyFAClientUndertaking') }}') }}.PATY_I, {{ ref('{{ ref('CpyFAClientUndertaking') }}') }}.PATY_I) AS PATY_I,
		CASE
			WHEN {{ ref('{{ ref('CpyFAClientUndertaking') }}') }}.REL_I IS NULL AND {{ ref('{{ ref('CpyFAClientUndertaking') }}') }}.REL_LEVL_C IS NULL THEN '1'
			WHEN {{ ref('{{ ref('CpyFAClientUndertaking') }}') }}.REL_I IS NULL AND {{ ref('{{ ref('CpyFAClientUndertaking') }}') }}.REL_LEVL_C IS NULL THEN '2'
			WHEN ({{ ref('{{ ref('CpyFAClientUndertaking') }}') }}.REL_I = {{ ref('{{ ref('CpyFAClientUndertaking') }}') }}.REL_I AND {{ ref('{{ ref('CpyFAClientUndertaking') }}') }}.REL_LEVL_C = {{ ref('{{ ref('CpyFAClientUndertaking') }}') }}.REL_LEVL_C) AND ({{ ref('{{ ref('CpyFAClientUndertaking') }}') }}.SRCE_SYST_REL_I <> {{ ref('{{ ref('CpyFAClientUndertaking') }}') }}.SRCE_SYST_REL_I OR {{ ref('{{ ref('CpyFAClientUndertaking') }}') }}.REL_TYPE_C <> {{ ref('{{ ref('CpyFAClientUndertaking') }}') }}.REL_TYPE_C OR {{ ref('{{ ref('CpyFAClientUndertaking') }}') }}.RELD_PATY_I <> {{ ref('{{ ref('CpyFAClientUndertaking') }}') }}.RELD_PATY_I OR {{ ref('{{ ref('CpyFAClientUndertaking') }}') }}.SRCE_SYST_C <> {{ ref('{{ ref('CpyFAClientUndertaking') }}') }}.SRCE_SYST_C OR {{ ref('{{ ref('CpyFAClientUndertaking') }}') }}.PATY_ROLE_C <> {{ ref('{{ ref('CpyFAClientUndertaking') }}') }}.PATY_ROLE_C OR {{ ref('{{ ref('CpyFAClientUndertaking') }}') }}.REL_STUS_C <> {{ ref('{{ ref('CpyFAClientUndertaking') }}') }}.REL_STUS_C OR {{ ref('{{ ref('CpyFAClientUndertaking') }}') }}.REL_EFFT_D <> {{ ref('{{ ref('CpyFAClientUndertaking') }}') }}.REL_EFFT_D OR {{ ref('{{ ref('CpyFAClientUndertaking') }}') }}.REL_EXPY_D <> {{ ref('{{ ref('CpyFAClientUndertaking') }}') }}.REL_EXPY_D OR {{ ref('{{ ref('CpyFAClientUndertaking') }}') }}.REL_REAS_C <> {{ ref('{{ ref('CpyFAClientUndertaking') }}') }}.REL_REAS_C OR {{ ref('{{ ref('CpyFAClientUndertaking') }}') }}.PATY_I <> {{ ref('{{ ref('CpyFAClientUndertaking') }}') }}.PATY_I) THEN '3'
			WHEN {{ ref('{{ ref('CpyFAClientUndertaking') }}') }}.REL_I = {{ ref('{{ ref('CpyFAClientUndertaking') }}') }}.REL_I AND {{ ref('{{ ref('CpyFAClientUndertaking') }}') }}.REL_LEVL_C = {{ ref('{{ ref('CpyFAClientUndertaking') }}') }}.REL_LEVL_C AND {{ ref('{{ ref('CpyFAClientUndertaking') }}') }}.SRCE_SYST_REL_I = {{ ref('{{ ref('CpyFAClientUndertaking') }}') }}.SRCE_SYST_REL_I AND {{ ref('{{ ref('CpyFAClientUndertaking') }}') }}.REL_TYPE_C = {{ ref('{{ ref('CpyFAClientUndertaking') }}') }}.REL_TYPE_C AND {{ ref('{{ ref('CpyFAClientUndertaking') }}') }}.RELD_PATY_I = {{ ref('{{ ref('CpyFAClientUndertaking') }}') }}.RELD_PATY_I AND {{ ref('{{ ref('CpyFAClientUndertaking') }}') }}.SRCE_SYST_C = {{ ref('{{ ref('CpyFAClientUndertaking') }}') }}.SRCE_SYST_C AND {{ ref('{{ ref('CpyFAClientUndertaking') }}') }}.PATY_ROLE_C = {{ ref('{{ ref('CpyFAClientUndertaking') }}') }}.PATY_ROLE_C AND {{ ref('{{ ref('CpyFAClientUndertaking') }}') }}.REL_STUS_C = {{ ref('{{ ref('CpyFAClientUndertaking') }}') }}.REL_STUS_C AND {{ ref('{{ ref('CpyFAClientUndertaking') }}') }}.REL_EFFT_D = {{ ref('{{ ref('CpyFAClientUndertaking') }}') }}.REL_EFFT_D AND {{ ref('{{ ref('CpyFAClientUndertaking') }}') }}.REL_EXPY_D = {{ ref('{{ ref('CpyFAClientUndertaking') }}') }}.REL_EXPY_D AND {{ ref('{{ ref('CpyFAClientUndertaking') }}') }}.REL_REAS_C = {{ ref('{{ ref('CpyFAClientUndertaking') }}') }}.REL_REAS_C AND {{ ref('{{ ref('CpyFAClientUndertaking') }}') }}.PATY_I = {{ ref('{{ ref('CpyFAClientUndertaking') }}') }}.PATY_I THEN '0'
		END AS change_code 
	FROM {{ ref('{{ ref('CpyFAClientUndertaking') }}') }} 
	FULL OUTER JOIN {{ ref('{{ ref('CpyFAClientUndertaking') }}') }} 
	ON {{ ref('{{ ref('CpyFAClientUndertaking') }}') }}.REL_I = {{ ref('{{ ref('CpyFAClientUndertaking') }}') }}.REL_I
	AND {{ ref('{{ ref('CpyFAClientUndertaking') }}') }}.REL_LEVL_C = {{ ref('{{ ref('CpyFAClientUndertaking') }}') }}.REL_LEVL_C
	WHERE change_code = '1' OR change_code = '3'
)

SELECT * FROM ChangeCapture