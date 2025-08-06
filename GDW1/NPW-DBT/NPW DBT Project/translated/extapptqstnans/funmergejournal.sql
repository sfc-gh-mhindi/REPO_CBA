{{ config(materialized='view', tags=['ExtApptQstnAns']) }}

WITH FunMergeJournal AS (
	SELECT
		APP_ID as APP_ID,
		SUBTYPE_CODE as SUBTYPE_CODE,
		QA_QUESTION_ID as QA_QUESTION_ID,
		QA_ANSWER_ID as QA_ANSWER_ID,
		TEXT_ANSWER as TEXT_ANSWER,
		CIF_CODE as CIF_CODE,
		ORIG_ETL_D
	FROM {{ ref('XfmSeparateRejectWithoutSourceAndTheRest__InSourceRec') }}
	UNION ALL
	SELECT
		APP_ID,
		SUBTYPE_CODE,
		QA_QUESTION_ID,
		QA_ANSWER_ID,
		TEXT_ANSWER,
		CIF_CODE,
		ORIG_ETL_D
	FROM {{ ref('XfmSeparateRejectWithoutSourceAndTheRest__InRejectWithoutSourceRec') }}
)

SELECT * FROM FunMergeJournal