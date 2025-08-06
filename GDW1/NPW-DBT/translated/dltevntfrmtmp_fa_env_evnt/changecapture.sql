{{ config(materialized='view', tags=['DltEVNTFrmTMP_FA_ENV_EVNT']) }}

WITH ChangeCapture AS (
	SELECT
		COALESCE({{ ref('{{ ref('CpyTmpFAEnvEvntTera') }}') }}.EVNT_I, {{ ref('{{ ref('CpyTmpFAEnvEvntTera') }}') }}.EVNT_I) AS EVNT_I,
		CASE
			WHEN {{ ref('{{ ref('CpyTmpFAEnvEvntTera') }}') }}.EVNT_I IS NULL THEN '1'
			WHEN {{ ref('{{ ref('CpyTmpFAEnvEvntTera') }}') }}.EVNT_I IS NULL THEN '2'
			WHEN ({{ ref('{{ ref('CpyTmpFAEnvEvntTera') }}') }}.EVNT_I = {{ ref('{{ ref('CpyTmpFAEnvEvntTera') }}') }}.EVNT_I) AND (NULL) THEN '3'
			WHEN {{ ref('{{ ref('CpyTmpFAEnvEvntTera') }}') }}.EVNT_I = {{ ref('{{ ref('CpyTmpFAEnvEvntTera') }}') }}.EVNT_I THEN '0'
		END AS change_code 
	FROM {{ ref('{{ ref('CpyTmpFAEnvEvntTera') }}') }} 
	FULL OUTER JOIN {{ ref('{{ ref('CpyTmpFAEnvEvntTera') }}') }} 
	ON {{ ref('{{ ref('CpyTmpFAEnvEvntTera') }}') }}.EVNT_I = {{ ref('{{ ref('CpyTmpFAEnvEvntTera') }}') }}.EVNT_I
	WHERE change_code = '1'
)

SELECT * FROM ChangeCapture