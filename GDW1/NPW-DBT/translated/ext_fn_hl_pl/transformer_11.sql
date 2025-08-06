{{ config(materialized='view', tags=['Ext_Fn_Hl_PL']) }}

WITH Transformer_11 AS (
	SELECT
		RECORD_TYPE,
		MOD_TIMESTAMP,
		HL_APP_ID,
		QA_QUESTION_ID,
		QA_ANSWER_ID,
		TEXT_ANSWER,
		CIF_CODE,
		CBA_STAFF_NUMBER,
		LODGEMENT_BRANCH_ID,
		SUBTYPE_CODE,
		'Z' AS YN_FLAG_ANSWER,
		Dummy
	FROM {{ ref('Data_Set_PL') }}
	WHERE 
)

SELECT * FROM Transformer_11