{{ config(materialized='view', tags=['DltEVNT_INT_GRUPFrmTMP_FA_ENV_EVNT']) }}

WITH ChangeCapture AS (
	SELECT
		COALESCE({{ ref('{{ ref('CpyTmpFAEnvEvntTera') }}') }}.INT_GRUP_I, {{ ref('{{ ref('CpyTmpFAEnvEvntTera') }}') }}.INT_GRUP_I) AS INT_GRUP_I,
		COALESCE({{ ref('{{ ref('CpyTmpFAEnvEvntTera') }}') }}.EVNT_I, {{ ref('{{ ref('CpyTmpFAEnvEvntTera') }}') }}.EVNT_I) AS EVNT_I,
		CASE
			WHEN {{ ref('{{ ref('CpyTmpFAEnvEvntTera') }}') }}.INT_GRUP_I IS NULL AND {{ ref('{{ ref('CpyTmpFAEnvEvntTera') }}') }}.EVNT_I IS NULL THEN '1'
			WHEN {{ ref('{{ ref('CpyTmpFAEnvEvntTera') }}') }}.INT_GRUP_I IS NULL AND {{ ref('{{ ref('CpyTmpFAEnvEvntTera') }}') }}.EVNT_I IS NULL THEN '2'
			WHEN ({{ ref('{{ ref('CpyTmpFAEnvEvntTera') }}') }}.INT_GRUP_I = {{ ref('{{ ref('CpyTmpFAEnvEvntTera') }}') }}.INT_GRUP_I AND {{ ref('{{ ref('CpyTmpFAEnvEvntTera') }}') }}.EVNT_I = {{ ref('{{ ref('CpyTmpFAEnvEvntTera') }}') }}.EVNT_I) AND (NULL) THEN '3'
			WHEN {{ ref('{{ ref('CpyTmpFAEnvEvntTera') }}') }}.INT_GRUP_I = {{ ref('{{ ref('CpyTmpFAEnvEvntTera') }}') }}.INT_GRUP_I AND {{ ref('{{ ref('CpyTmpFAEnvEvntTera') }}') }}.EVNT_I = {{ ref('{{ ref('CpyTmpFAEnvEvntTera') }}') }}.EVNT_I THEN '0'
		END AS change_code 
	FROM {{ ref('{{ ref('CpyTmpFAEnvEvntTera') }}') }} 
	FULL OUTER JOIN {{ ref('{{ ref('CpyTmpFAEnvEvntTera') }}') }} 
	ON {{ ref('{{ ref('CpyTmpFAEnvEvntTera') }}') }}.INT_GRUP_I = {{ ref('{{ ref('CpyTmpFAEnvEvntTera') }}') }}.INT_GRUP_I
	AND {{ ref('{{ ref('CpyTmpFAEnvEvntTera') }}') }}.EVNT_I = {{ ref('{{ ref('CpyTmpFAEnvEvntTera') }}') }}.EVNT_I
	WHERE change_code = '1'
)

SELECT * FROM ChangeCapture