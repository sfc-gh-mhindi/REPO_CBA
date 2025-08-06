{{ config(materialized='view', tags=['XfmClpAppProdAnsExt']) }}

WITH Identify_IDs_ComPlAppProd AS (
	SELECT
		-- *SRC*: \(20)IF trim(Len(( IF IsNotNull((InComAppProdSeq.QA_QUESTION_ID)) THEN (InComAppProdSeq.QA_QUESTION_ID) ELSE ""))) = '0' Then 'N' Else 'Y',
		IFF(TRIM(LEN(IFF({{ ref('SrcComAppProdSeq') }}.QA_QUESTION_ID IS NOT NULL, {{ ref('SrcComAppProdSeq') }}.QA_QUESTION_ID, ''))) = '0', 'N', 'Y') AS LOAD,
		APP_ID,
		APP_PROD_ID,
		SUBTYPE_CODE,
		QA_QUESTION_ID,
		QA_ANSWER_ID,
		TEXT_ANSWER,
		-- *SRC*: SetNull(),
		SETNULL() AS CIF_CODE
	FROM {{ ref('SrcComAppProdSeq') }}
	WHERE LOAD = 'Y'
)

SELECT * FROM Identify_IDs_ComPlAppProd