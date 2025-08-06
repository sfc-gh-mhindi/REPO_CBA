{{ config(materialized='view', tags=['Ext_APP_ANS_XFERFrmExt']) }}

WITH LkpReferences AS (
	SELECT
		{{ ref('CpyRename') }}.HL_APP_ID,
		{{ ref('CpyRename') }}.QA_QUESTION_ID,
		{{ ref('CpyRename') }}.QA_ANSWER_ID,
		{{ ref('CpyRename') }}.TEXT_ANSWER,
		{{ ref('CpyRename') }}.CIF_CODE,
		{{ ref('CpyRename') }}.CBA_STAFF_NUMBER,
		{{ ref('CpyRename') }}.LODGEMENT_BRANCH_ID,
		{{ ref('CpyRename') }}.ORIG_ETL_D,
		{{ ref('SrcMAP_CSE_APPT_QLFYLks') }}.APPT_QLFY_C,
		{{ ref('CpyRename') }}.MOD_TIMESTAMP,
		DSLink270.HL_APP_ID,
		DSLink270.QA_QUESTION_ID,
		DSLink270.QA_ANSWER_ID,
		DSLink270.TEXT_ANSWER,
		DSLink270.CIF_CODE,
		DSLink270.CBA_STAFF_NUMBER,
		DSLink270.LODGEMENT_BRANCH_ID,
		DSLink270.SUBTYPE_CODE AS SBTY_CODE,
		DSLink270.MOD_TIMESTAMP,
		DSLink270.ORIG_ETL_D
	FROM {{ ref('CpyRename') }}
	LEFT JOIN {{ ref('SrcMAP_CSE_APPT_QLFYLks') }} ON 
)

SELECT * FROM LkpReferences