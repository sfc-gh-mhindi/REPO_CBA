{{ config(materialized='view', tags=['ExtAppCon_TuApp']) }}

WITH CpyInApptRelSeqSeq AS (
	SELECT
		SUBTYPE_CODE,
		HL_APP_PROD_ID,
		TU_APP_CONDITION_ID,
		TU_APP_CONDITION_CAT_ID,
		CONDITION_MET_DATE
	FROM {{ ref('SrcInApptRelSeqSeq') }}
)

SELECT * FROM CpyInApptRelSeqSeq