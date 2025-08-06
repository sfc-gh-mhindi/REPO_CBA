{{ config(materialized='view', tags=['XfmSmCaseStateFrmExt']) }}

WITH LkpReferences AS (
	SELECT
		{{ ref('CpyRename') }}.SM_CASE_STATE_ID,
		{{ ref('CpyRename') }}.SM_CASE_ID,
		{{ ref('CpyRename') }}.SM_STATE_CAT_ID,
		{{ ref('CpyRename') }}.START_DATE,
		{{ ref('CpyRename') }}.END_DATE,
		{{ ref('CpyRename') }}.CREATED_BY_STAFF_NUMBER,
		{{ ref('CpyRename') }}.STATE_CAUSED_BY_ACTION_ID,
		{{ ref('CpyRename') }}.ORIG_ETL_D,
		{{ ref('Join_222') }}.TARG_I AS targ_i,
		{{ ref('Join_222') }}.TARG_SUBJ AS targ_tabl,
		{{ ref('SrcMAP_CSE_SM_CASE_STUSLks') }}.STUS_C
	FROM {{ ref('CpyRename') }}
	LEFT JOIN {{ ref('SrcMAP_CSE_SM_CASE_STUSLks') }} ON 
	LEFT JOIN {{ ref('Join_222') }} ON {{ ref('CpyRename') }}.SM_CASE_ID = {{ ref('Join_222') }}.SM_CASE_ID
)

SELECT * FROM LkpReferences