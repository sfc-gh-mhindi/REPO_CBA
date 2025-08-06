{{ config(materialized='view', tags=['ExtAppCon_TuApp']) }}

WITH JoinSrcSortReject AS (
	SELECT
		{{ ref('XfmCheckInApptRelIdNulls__OutCheckApptRelIdNullsSorted') }}.SUBTYPE_CODE,
		{{ ref('XfmCheckInApptRelIdNulls__OutCheckApptRelIdNullsSorted') }}.HL_APP_PROD_ID,
		{{ ref('XfmCheckInApptRelIdNulls__OutCheckApptRelIdNullsSorted') }}.TU_APP_CONDITION_ID,
		{{ ref('XfmCheckInApptRelIdNulls__OutCheckApptRelIdNullsSorted') }}.TU_APP_CONDITION_CAT_ID,
		{{ ref('XfmCheckInApptRelIdNulls__OutCheckApptRelIdNullsSorted') }}.CONDITION_MET_DATE,
		{{ ref('CpyRejtApptRelRejectOra') }}.TU_APP_CONDITION_ID_R,
		{{ ref('CpyRejtApptRelRejectOra') }}.TU_APP_CONDITION_CAT_ID_R,
		{{ ref('CpyRejtApptRelRejectOra') }}.CONDITION_MET_DATE_R,
		{{ ref('CpyRejtApptRelRejectOra') }}.SUBTYPE_CODE_R,
		{{ ref('CpyRejtApptRelRejectOra') }}.HL_APP_PROD_ID AS HL_APP_PROD_ID_R,
		{{ ref('CpyRejtApptRelRejectOra') }}.ORIG_ETL_D_R
	FROM {{ ref('XfmCheckInApptRelIdNulls__OutCheckApptRelIdNullsSorted') }}
	OUTER JOIN {{ ref('CpyRejtApptRelRejectOra') }} ON {{ ref('XfmCheckInApptRelIdNulls__OutCheckApptRelIdNullsSorted') }}.HL_APP_PROD_ID = {{ ref('CpyRejtApptRelRejectOra') }}.HL_APP_PROD_ID
)

SELECT * FROM JoinSrcSortReject