{{ config(materialized='view', tags=['DltMAP_CSE_SM_CASEFrmMAP_CSE_SM_CASE_DS']) }}

WITH Rename AS (
	SELECT
		SM_CASE_ID,
		{{ ref('Tgt_MapCseSmCaseDS') }}.TARG_I AS NEW_TARG_I,
		{{ ref('Tgt_MapCseSmCaseDS') }}.TARG_SUBJ AS NEW_TARG_SUBJ,
		TARG_I,
		TARG_SUBJ
	FROM {{ ref('Tgt_MapCseSmCaseDS') }}
)

SELECT * FROM Rename