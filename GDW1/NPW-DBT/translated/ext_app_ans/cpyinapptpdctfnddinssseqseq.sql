{{ config(materialized='view', tags=['Ext_APP_ANS']) }}

WITH CpyInApptPdctFnddInssSeqSeq AS (
	SELECT
		MOD_TIMESTAMP,
		HL_APP_ID,
		QA_QUESTION_ID,
		QA_ANSWER_ID,
		TEXT_ANSWER,
		CIF_CODE,
		CBA_STAFF_NUMBER,
		LODGEMENT_BRANCH_ID,
		SUBTYPE_CODE,
		YN_FLAG_ANSWER
	FROM {{ ref('Data_Set_235') }}
)

SELECT * FROM CpyInApptPdctFnddInssSeqSeq