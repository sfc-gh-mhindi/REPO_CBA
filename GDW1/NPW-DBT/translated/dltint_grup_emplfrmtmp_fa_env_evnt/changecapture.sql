{{ config(materialized='view', tags=['DltINT_GRUP_EMPLFrmTMP_FA_ENV_EVNT']) }}

WITH ChangeCapture AS (
	SELECT
		COALESCE({{ ref('{{ ref('CpyTmpFAEnvEvntTera') }}') }}.INT_GRUP_I, {{ ref('{{ ref('CpyTmpFAEnvEvntTera') }}') }}.INT_GRUP_I) AS INT_GRUP_I,
		CASE
			WHEN {{ ref('{{ ref('CpyTmpFAEnvEvntTera') }}') }}.INT_GRUP_I IS NULL THEN '1'
			WHEN {{ ref('{{ ref('CpyTmpFAEnvEvntTera') }}') }}.INT_GRUP_I IS NULL THEN '2'
			WHEN ({{ ref('{{ ref('CpyTmpFAEnvEvntTera') }}') }}.INT_GRUP_I = {{ ref('{{ ref('CpyTmpFAEnvEvntTera') }}') }}.INT_GRUP_I) AND ({{ ref('{{ ref('CpyTmpFAEnvEvntTera') }}') }}.EMPL_I <> {{ ref('{{ ref('CpyTmpFAEnvEvntTera') }}') }}.EMPL_I) THEN '3'
			WHEN {{ ref('{{ ref('CpyTmpFAEnvEvntTera') }}') }}.INT_GRUP_I = {{ ref('{{ ref('CpyTmpFAEnvEvntTera') }}') }}.INT_GRUP_I AND {{ ref('{{ ref('CpyTmpFAEnvEvntTera') }}') }}.EMPL_I = {{ ref('{{ ref('CpyTmpFAEnvEvntTera') }}') }}.EMPL_I THEN '0'
		END AS change_code 
	FROM {{ ref('{{ ref('CpyTmpFAEnvEvntTera') }}') }} 
	FULL OUTER JOIN {{ ref('{{ ref('CpyTmpFAEnvEvntTera') }}') }} 
	ON {{ ref('{{ ref('CpyTmpFAEnvEvntTera') }}') }}.INT_GRUP_I = {{ ref('{{ ref('CpyTmpFAEnvEvntTera') }}') }}.INT_GRUP_I
	WHERE change_code = '1' OR change_code = '3'
)

SELECT * FROM ChangeCapture