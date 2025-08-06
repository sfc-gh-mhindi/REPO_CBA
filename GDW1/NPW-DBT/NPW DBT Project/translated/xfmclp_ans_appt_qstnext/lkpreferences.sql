{{ config(materialized='view', tags=['XfmCLP_ANS_Appt_QstnExt']) }}

WITH LkpReferences AS (
	SELECT
		{{ ref('CpyRename') }}.APP_ID,
		{{ ref('SrcMAP_CSE_APPT_QLFYLks') }}.APPT_QLFY_C,
		{{ ref('SrcMAP_CSE_APPT_QSTNks') }}.QSTN_ID,
		{{ ref('CpyRename') }}.QA_ANSWER_ID,
		{{ ref('CpyRename') }}.TEXT_ANSWER,
		{{ ref('CpyRename') }}.CIF_CODE,
		{{ ref('SrcMAP_CSE_APPT_QSTNks') }}.ROW_S
	FROM {{ ref('CpyRename') }}
	LEFT JOIN {{ ref('SrcMAP_CSE_APPT_QLFYLks') }} ON 
	LEFT JOIN {{ ref('SrcMAP_CSE_APPT_QSTNks') }} ON 
)

SELECT * FROM LkpReferences