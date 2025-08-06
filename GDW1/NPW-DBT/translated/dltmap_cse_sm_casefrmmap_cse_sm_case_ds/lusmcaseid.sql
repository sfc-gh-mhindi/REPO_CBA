{{ config(materialized='view', tags=['DltMAP_CSE_SM_CASEFrmMAP_CSE_SM_CASE_DS']) }}

WITH LuSmCaseId AS (
	SELECT
		{{ ref('Rename') }}.SM_CASE_ID,
		{{ ref('Rename') }}.NEW_TARG_I AS TARG_I,
		{{ ref('Rename') }}.NEW_TARG_SUBJ AS TARG_SUBJ,
		OutMapCseSmCaseDS.SM_CASE_ID,
		OutMapCseSmCaseDS.TARG_I AS NEW_TARG_I,
		OutMapCseSmCaseDS.TARG_SUBJ AS NEW_TARG_SUBJ
	FROM {{ ref('Rename') }}
	LEFT JOIN {{ ref('Join_81') }} ON {{ ref('Rename') }}.SM_CASE_ID = {{ ref('Join_81') }}.SM_CASE_ID
)

SELECT * FROM LuSmCaseId