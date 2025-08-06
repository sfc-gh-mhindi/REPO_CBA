{{ config(materialized='view', tags=['DltINT_GRUP_UNID_PATYFrmTMP_FA_PROP_CLNT']) }}

WITH ChangeCapture AS (
	SELECT
		COALESCE({{ ref('{{ ref('CpyFAPropClnt') }}') }}.INT_GRUP_I, {{ ref('{{ ref('CpyFAPropClnt') }}') }}.INT_GRUP_I) AS INT_GRUP_I,
		COALESCE({{ ref('{{ ref('CpyFAPropClnt') }}') }}.SRCE_SYST_PATY_I, {{ ref('{{ ref('CpyFAPropClnt') }}') }}.SRCE_SYST_PATY_I) AS SRCE_SYST_PATY_I,
		CASE
			WHEN {{ ref('{{ ref('CpyFAPropClnt') }}') }}.INT_GRUP_I IS NULL AND {{ ref('{{ ref('CpyFAPropClnt') }}') }}.SRCE_SYST_PATY_I IS NULL THEN '1'
			WHEN {{ ref('{{ ref('CpyFAPropClnt') }}') }}.INT_GRUP_I IS NULL AND {{ ref('{{ ref('CpyFAPropClnt') }}') }}.SRCE_SYST_PATY_I IS NULL THEN '2'
			WHEN ({{ ref('{{ ref('CpyFAPropClnt') }}') }}.INT_GRUP_I = {{ ref('{{ ref('CpyFAPropClnt') }}') }}.INT_GRUP_I AND {{ ref('{{ ref('CpyFAPropClnt') }}') }}.SRCE_SYST_PATY_I = {{ ref('{{ ref('CpyFAPropClnt') }}') }}.SRCE_SYST_PATY_I) AND ({{ ref('{{ ref('CpyFAPropClnt') }}') }}.UNID_PATY_M <> {{ ref('{{ ref('CpyFAPropClnt') }}') }}.UNID_PATY_M OR {{ ref('{{ ref('CpyFAPropClnt') }}') }}.PATY_TYPE_C <> {{ ref('{{ ref('CpyFAPropClnt') }}') }}.PATY_TYPE_C) THEN '3'
			WHEN {{ ref('{{ ref('CpyFAPropClnt') }}') }}.INT_GRUP_I = {{ ref('{{ ref('CpyFAPropClnt') }}') }}.INT_GRUP_I AND {{ ref('{{ ref('CpyFAPropClnt') }}') }}.SRCE_SYST_PATY_I = {{ ref('{{ ref('CpyFAPropClnt') }}') }}.SRCE_SYST_PATY_I AND {{ ref('{{ ref('CpyFAPropClnt') }}') }}.UNID_PATY_M = {{ ref('{{ ref('CpyFAPropClnt') }}') }}.UNID_PATY_M AND {{ ref('{{ ref('CpyFAPropClnt') }}') }}.PATY_TYPE_C = {{ ref('{{ ref('CpyFAPropClnt') }}') }}.PATY_TYPE_C THEN '0'
		END AS change_code 
	FROM {{ ref('{{ ref('CpyFAPropClnt') }}') }} 
	FULL OUTER JOIN {{ ref('{{ ref('CpyFAPropClnt') }}') }} 
	ON {{ ref('{{ ref('CpyFAPropClnt') }}') }}.INT_GRUP_I = {{ ref('{{ ref('CpyFAPropClnt') }}') }}.INT_GRUP_I
	AND {{ ref('{{ ref('CpyFAPropClnt') }}') }}.SRCE_SYST_PATY_I = {{ ref('{{ ref('CpyFAPropClnt') }}') }}.SRCE_SYST_PATY_I
	WHERE change_code = '1' OR change_code = '3' OR change_code = '0'
)

SELECT * FROM ChangeCapture