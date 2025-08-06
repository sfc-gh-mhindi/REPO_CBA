{{ config(materialized='view', tags=['ExtCCL_HL_APP']) }}

WITH JoinSrcSortReject AS (
	SELECT
		{{ ref('XfmCheckInApptRelIdNulls__OutCheckApptRelIdNullsSorted') }}.CCL_HL_APP_ID,
		{{ ref('XfmCheckInApptRelIdNulls__OutCheckApptRelIdNullsSorted') }}.CCL_APP_ID,
		{{ ref('XfmCheckInApptRelIdNulls__OutCheckApptRelIdNullsSorted') }}.HL_APP_ID,
		{{ ref('XfmCheckInApptRelIdNulls__OutCheckApptRelIdNullsSorted') }}.LMI_AMT,
		{{ ref('XfmCheckInApptRelIdNulls__OutCheckApptRelIdNullsSorted') }}.HL_PACKAGE_CAT_ID,
		{{ ref('CpyRejtApptRelRejectOra') }}.CCL_HL_APP_ID AS CCL_HL_APP_ID_R,
		{{ ref('CpyRejtApptRelRejectOra') }}.CCL_APP_ID_R,
		{{ ref('CpyRejtApptRelRejectOra') }}.HL_APP_ID_R,
		{{ ref('CpyRejtApptRelRejectOra') }}.LMI_AMT_R,
		{{ ref('CpyRejtApptRelRejectOra') }}.HL_PACKAGE_CAT_ID_R,
		{{ ref('CpyRejtApptRelRejectOra') }}.ORIG_ETL_D_R
	FROM {{ ref('XfmCheckInApptRelIdNulls__OutCheckApptRelIdNullsSorted') }}
	OUTER JOIN {{ ref('CpyRejtApptRelRejectOra') }} ON {{ ref('XfmCheckInApptRelIdNulls__OutCheckApptRelIdNullsSorted') }}.CCL_HL_APP_ID = {{ ref('CpyRejtApptRelRejectOra') }}.CCL_HL_APP_ID
)

SELECT * FROM JoinSrcSortReject