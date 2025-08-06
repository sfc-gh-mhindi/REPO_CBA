{{ config(materialized='view', tags=['MergeReasonSmCaseState']) }}

WITH JoinSmCaseStateReason AS (
	SELECT
		{{ ref('CgAdd_Flag1') }}.SCS_RECORD_TYPE,
		{{ ref('CgAdd_Flag1') }}.SCS_MOD_TIMESTAMP,
		{{ ref('CgAdd_Flag1') }}.SM_CASE_STATE_ID,
		{{ ref('CgAdd_Flag1') }}.SCS_SM_CASE_ID,
		{{ ref('CgAdd_Flag1') }}.SCS_SM_STATE_CAT_ID,
		{{ ref('CgAdd_Flag1') }}.SCS_START_DATE,
		{{ ref('CgAdd_Flag1') }}.SCS_END_DATE,
		{{ ref('CgAdd_Flag1') }}.SCS_CREATED_BY_STAFF_NUMBER,
		{{ ref('CgAdd_Flag1') }}.SCS_STATE_CAUSED_BY_ACTION_ID,
		{{ ref('CgAdd_Flag1') }}.SCS_DUMMY,
		{{ ref('CgAdd_Flag1') }}.SM_CASE_STATE_FOUND_FLAG,
		{{ ref('CgAdd_Flag2') }}.SCSR_RECORD_TYPE,
		{{ ref('CgAdd_Flag2') }}.SCSR_MOD_TIMESTAMP,
		{{ ref('CgAdd_Flag2') }}.SCSR_SM_CASE_STATE_REASON_ID,
		{{ ref('CgAdd_Flag2') }}.SCSR_SM_REASON_CAT_ID,
		{{ ref('CgAdd_Flag2') }}.SCSR_DUMMY,
		{{ ref('CgAdd_Flag2') }}.SM_CASE_STATE_REAS_FOUND_FLAG
	FROM {{ ref('CgAdd_Flag1') }}
	INNER JOIN {{ ref('CgAdd_Flag2') }} ON {{ ref('CgAdd_Flag1') }}.SM_CASE_STATE_ID = {{ ref('CgAdd_Flag2') }}.SM_CASE_STATE_ID
)

SELECT * FROM JoinSmCaseStateReason