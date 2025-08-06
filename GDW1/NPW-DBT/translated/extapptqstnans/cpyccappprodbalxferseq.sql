{{ config(materialized='view', tags=['ExtApptQstnAns']) }}

WITH CpyCCAppProdBalXferSeq AS (
	SELECT
		APP_ID,
		SUBTYPE_CODE,
		QA_QUESTION_ID,
		QA_ANSWER_ID,
		TEXT_ANSWER,
		CIF_CODE
	FROM {{ ref('Data_Set_215') }}
)

SELECT * FROM CpyCCAppProdBalXferSeq