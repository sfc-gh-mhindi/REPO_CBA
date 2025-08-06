{{ config(materialized='view', tags=['ExtSmCaseState']) }}

WITH XfmCheckSmCaseStateIdNulls__OutCheckSmCaseStateIdNullsSorted AS (
	SELECT
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((XfmCheckSmCaseStateIdNulls.SM_CASE_STATE_ID)) THEN (XfmCheckSmCaseStateIdNulls.SM_CASE_STATE_ID) ELSE ""))) = 0) Then 'REJ5004' else '',
		IFF(LEN(TRIM(IFF({{ ref('Cpy_NoOp') }}.SM_CASE_STATE_ID IS NOT NULL, {{ ref('Cpy_NoOp') }}.SM_CASE_STATE_ID, ''))) = 0, 'REJ5004', '') AS ErrorCode,
		SM_CASE_STATE_ID,
		SM_CASE_ID,
		SM_STATE_CAT_ID,
		START_DATE,
		END_DATE,
		CREATED_BY_STAFF_NUMBER,
		STATE_CAUSED_BY_ACTION_ID
	FROM {{ ref('Cpy_NoOp') }}
	WHERE SUBSTRING(ErrorCode, 1, 3) <> 'REJ'
)

SELECT * FROM XfmCheckSmCaseStateIdNulls__OutCheckSmCaseStateIdNullsSorted