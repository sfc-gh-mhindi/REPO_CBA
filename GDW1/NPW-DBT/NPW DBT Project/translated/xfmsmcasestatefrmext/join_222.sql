{{ config(materialized='view', tags=['XfmSmCaseStateFrmExt']) }}

WITH Join_222 AS (
	SELECT
		{{ ref('Data_Set_221') }}.SM_CASE_ID,
		{{ ref('Data_Set_221') }}.TARG_I,
		{{ ref('Data_Set_221') }}.TARG_SUBJ
	FROM {{ ref('Data_Set_221') }}
	INNER JOIN {{ ref('CpyRename') }} ON {{ ref('Data_Set_221') }}.SM_CASE_ID = {{ ref('CpyRename') }}.SM_CASE_ID
)

SELECT * FROM Join_222