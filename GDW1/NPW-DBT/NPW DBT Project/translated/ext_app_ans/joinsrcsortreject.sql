{{ config(materialized='view', tags=['Ext_APP_ANS']) }}

WITH JoinSrcSortReject AS (
	SELECT
		{{ ref('XfmCheckInApptPdctFnddInssdNulls__OutCheckApptPdctFnddInssdNullsSorted') }}.HL_APP_ID,
		{{ ref('XfmCheckInApptPdctFnddInssdNulls__OutCheckApptPdctFnddInssdNullsSorted') }}.QA_QUESTION_ID,
		{{ ref('XfmCheckInApptPdctFnddInssdNulls__OutCheckApptPdctFnddInssdNullsSorted') }}.QA_ANSWER_ID,
		{{ ref('XfmCheckInApptPdctFnddInssdNulls__OutCheckApptPdctFnddInssdNullsSorted') }}.TEXT_ANSWER,
		{{ ref('XfmCheckInApptPdctFnddInssdNulls__OutCheckApptPdctFnddInssdNullsSorted') }}.CIF_CODE,
		{{ ref('XfmCheckInApptPdctFnddInssdNulls__OutCheckApptPdctFnddInssdNullsSorted') }}.CBA_STAFF_NUMBER,
		{{ ref('XfmCheckInApptPdctFnddInssdNulls__OutCheckApptPdctFnddInssdNullsSorted') }}.LODGEMENT_BRANCH_ID,
		{{ ref('XfmCheckInApptPdctFnddInssdNulls__OutCheckApptPdctFnddInssdNullsSorted') }}.SUBTYPE_CODE,
		{{ ref('XfmCheckInApptPdctFnddInssdNulls__OutCheckApptPdctFnddInssdNullsSorted') }}.MOD_TIMESTAMP,
		{{ ref('CpyRejtApptPdctFnddInssRejectOra') }}.HL_APP_ID AS HL_APP_ID_R,
		{{ ref('CpyRejtApptPdctFnddInssRejectOra') }}.QA_QUESTION_ID_R,
		{{ ref('CpyRejtApptPdctFnddInssRejectOra') }}.QA_ANSWER_ID_R,
		{{ ref('CpyRejtApptPdctFnddInssRejectOra') }}.TEXT_ANSWER_R,
		{{ ref('CpyRejtApptPdctFnddInssRejectOra') }}.CIF_CODE_R,
		{{ ref('CpyRejtApptPdctFnddInssRejectOra') }}.CBA_STAFF_NUMBER_R,
		{{ ref('CpyRejtApptPdctFnddInssRejectOra') }}.LODGEMENT_BRANCH_ID_R,
		{{ ref('CpyRejtApptPdctFnddInssRejectOra') }}.SUBTYPE_CODE_R,
		{{ ref('CpyRejtApptPdctFnddInssRejectOra') }}.MOD_TIMESTAMP_R,
		{{ ref('CpyRejtApptPdctFnddInssRejectOra') }}.ORIG_ETL_D_R
	FROM {{ ref('XfmCheckInApptPdctFnddInssdNulls__OutCheckApptPdctFnddInssdNullsSorted') }}
	OUTER JOIN {{ ref('CpyRejtApptPdctFnddInssRejectOra') }} ON {{ ref('XfmCheckInApptPdctFnddInssdNulls__OutCheckApptPdctFnddInssdNullsSorted') }}.HL_APP_ID = {{ ref('CpyRejtApptPdctFnddInssRejectOra') }}.HL_APP_ID
)

SELECT * FROM JoinSrcSortReject