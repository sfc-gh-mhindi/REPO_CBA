{{ config(materialized='view', tags=['ExtReasonSmCaseState']) }}

WITH XfmCheckSmCaseStateIdNulls__OutCheckSmCaseStateIdNullsSorted AS (
	SELECT
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((XfmCheckSmCaseStateIdNulls.SM_CASE_STATE_ID)) THEN (XfmCheckSmCaseStateIdNulls.SM_CASE_STATE_ID) ELSE ""))) = 0) Then 'REJ5004' else '',
		IFF(LEN(TRIM(IFF({{ ref('Cpy_NoOp') }}.SM_CASE_STATE_ID IS NOT NULL, {{ ref('Cpy_NoOp') }}.SM_CASE_STATE_ID, ''))) = 0, 'REJ5004', '') AS ErrorCode,
		SM_CASE_STATE_ID,
		SCS_SM_CASE_ID,
		SCS_SM_STATE_CAT_ID,
		SCS_START_DATE,
		SCS_END_DATE,
		SCS_CREATED_BY_STAFF_NUMBER,
		SCS_STATE_CAUSED_BY_ACTION_ID,
		SCSR_SM_CASE_STATE_REASON_ID,
		SCSR_SM_REASON_CAT_ID,
		SM_CASE_STATE_REAS_FOUND_FLAG,
		SM_CASE_STATE_FOUND_FLAG
	FROM {{ ref('Cpy_NoOp') }}
	WHERE SUBSTRING(ErrorCode, 1, 3) <> 'REJ'
)

SELECT * FROM XfmCheckSmCaseStateIdNulls__OutCheckSmCaseStateIdNullsSorted