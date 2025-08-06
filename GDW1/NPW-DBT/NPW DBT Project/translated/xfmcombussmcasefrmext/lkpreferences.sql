{{ config(materialized='view', tags=['XfmComBusSmCaseFrmExt']) }}

WITH LkpReferences AS (
	SELECT
		{{ ref('CpyRename') }}.SM_CASE_ID,
		{{ ref('CpyRename') }}.CREATED_TIMESTAMP,
		{{ ref('CpyRename') }}.WIM_PROCESS_ID,
		{{ ref('CpyRename') }}.ORIG_ETL_D,
		{{ ref('Join_212') }}.TARG_I,
		{{ ref('Join_212') }}.TARG_SUBJ
	FROM {{ ref('CpyRename') }}
	LEFT JOIN {{ ref('Join_212') }} ON {{ ref('CpyRename') }}.SM_CASE_ID = {{ ref('Join_212') }}.SM_CASE_ID
)

SELECT * FROM LkpReferences