{{ config(materialized='view', tags=['XfmReasonSmCaseStateFrmExt']) }}

WITH Join_221 AS (
	SELECT
		{{ ref('Data_Set_220') }}.SM_CASE_ID,
		{{ ref('Data_Set_220') }}.TARG_I,
		{{ ref('Data_Set_220') }}.TARG_SUBJ
	FROM {{ ref('Data_Set_220') }}
	INNER JOIN {{ ref('Transformer_224') }} ON {{ ref('Data_Set_220') }}.SM_CASE_ID = {{ ref('Transformer_224') }}.SM_CASE_ID
)

SELECT * FROM Join_221