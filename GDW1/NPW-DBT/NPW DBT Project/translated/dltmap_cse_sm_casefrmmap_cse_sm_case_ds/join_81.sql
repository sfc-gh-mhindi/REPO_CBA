{{ config(materialized='view', tags=['DltMAP_CSE_SM_CASEFrmMAP_CSE_SM_CASE_DS']) }}

WITH Join_81 AS (
	SELECT
		{{ ref('Data_Set_80') }}.SM_CASE_ID,
		{{ ref('Data_Set_80') }}.TARG_I,
		{{ ref('Data_Set_80') }}.TARG_SUBJ
	FROM {{ ref('Data_Set_80') }}
	INNER JOIN {{ ref('Rename') }} ON {{ ref('Data_Set_80') }}.SM_CASE_ID = {{ ref('Rename') }}.SM_CASE_ID
)

SELECT * FROM Join_81