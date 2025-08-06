{{ config(materialized='view', tags=['ExtSmCaseState']) }}

WITH Cpy_NoOp AS (
	SELECT
		SM_CASE_STATE_ID,
		SM_CASE_ID,
		SM_STATE_CAT_ID,
		START_DATE,
		END_DATE,
		CREATED_BY_STAFF_NUMBER,
		STATE_CAUSED_BY_ACTION_ID
	FROM {{ ref('SrcSmCaseStateSeq') }}
)

SELECT * FROM Cpy_NoOp