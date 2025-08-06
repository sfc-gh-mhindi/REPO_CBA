{{ config(materialized='view', tags=['XfmReasonSmCaseStateFrmExt']) }}

WITH LkpReferences AS (
	SELECT
		{{ ref('CpyRename') }}.SM_CASE_STATE_ID,
		{{ ref('CpyRename') }}.SM_CASE_ID,
		{{ ref('CpyRename') }}.SM_STATE_CAT_ID,
		{{ ref('CpyRename') }}.SCS_START_DATE,
		{{ ref('CpyRename') }}.SCS_END_DATE,
		{{ ref('CpyRename') }}.SCS_CREATED_BY_STAFF_NUMBER,
		{{ ref('CpyRename') }}.SCS_STATE_CAUSED_BY_ACTION_ID,
		{{ ref('CpyRename') }}.SCSR_SM_CASE_STATE_REASON_ID,
		{{ ref('CpyRename') }}.SM_REAS_CAT_ID,
		{{ ref('CpyRename') }}.SM_CASE_STATE_REAS_FOUND_FLAG,
		{{ ref('CpyRename') }}.SM_CASE_STATE_FOUND_FLAG,
		{{ ref('CpyRename') }}.ORIG_ETL_D,
		{{ ref('SrcMAP_CSE_SM_CASE_STUS_REASLks') }}.STUS_REAS_TYPE_C,
		{{ ref('SrcMAP_CSE_SM_CASE_STUSLks') }}.STUS_C,
		{{ ref('Join_221') }}.TARG_I,
		{{ ref('Join_221') }}.TARG_SUBJ
	FROM {{ ref('CpyRename') }}
	LEFT JOIN {{ ref('SrcMAP_CSE_SM_CASE_STUS_REASLks') }} ON 
	LEFT JOIN {{ ref('SrcMAP_CSE_SM_CASE_STUSLks') }} ON 
	LEFT JOIN {{ ref('Join_221') }} ON {{ ref('CpyRename') }}.SM_CASE_ID = {{ ref('Join_221') }}.SM_CASE_ID
)

SELECT * FROM LkpReferences