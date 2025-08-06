{{ config(materialized='view', tags=['DltINT_GRUP_EMPLFrmTMP_FA_UNDERTAKING']) }}

WITH JoinAll AS (
	SELECT
		{{ ref('ChangeCapture') }}.INT_GRUP_I,
		{{ ref('ChangeCapture') }}.change_code,
		{{ ref('CpyFAUtak') }}.EMPL_I,
		{{ ref('CpyFAUtak') }}.EMPL_ROLE_C
	FROM {{ ref('ChangeCapture') }}
	INNER JOIN {{ ref('CpyFAUtak') }} ON {{ ref('ChangeCapture') }}.INT_GRUP_I = {{ ref('CpyFAUtak') }}.INT_GRUP_I
)

SELECT * FROM JoinAll