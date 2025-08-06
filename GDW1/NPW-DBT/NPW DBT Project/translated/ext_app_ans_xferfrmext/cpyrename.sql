{{ config(materialized='view', tags=['Ext_APP_ANS_XFERFrmExt']) }}

WITH CpyRename AS (
	SELECT
		HL_APP_ID,
		QA_QUESTION_ID,
		QA_ANSWER_ID,
		TEXT_ANSWER,
		CIF_CODE,
		CBA_STAFF_NUMBER,
		LODGEMENT_BRANCH_ID,
		{{ ref('SrcApptPdctFnddInssPremapDS') }}.SUBTYPE_CODE AS SBTY_CODE,
		MOD_TIMESTAMP,
		ORIG_ETL_D
	FROM {{ ref('SrcApptPdctFnddInssPremapDS') }}
)

SELECT * FROM CpyRename