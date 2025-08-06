{{ config(materialized='view', tags=['XfmClpCntAnsExt']) }}

WITH Identify_IDs_ComPlAppProd AS (
	SELECT
		LOAD,
		APP_ID,
		APP_PROD_ID,
		SUBTYPE_CODE,
		QA_QUESTION_ID,
		QA_ANSWER_ID,
		TEXT_ANSWER,
		CIF_CODE
	FROM {{ ref('SrcComAppProdSeq') }}
	WHERE 
)

SELECT * FROM Identify_IDs_ComPlAppProd