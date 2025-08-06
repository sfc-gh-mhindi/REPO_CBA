{{ config(materialized='view', tags=['ExtComBusSmCase']) }}

WITH JoinSrcSortReject AS (
	SELECT
		{{ ref('XfmCheckIdNull__OutCheckIdNullSorted') }}.SM_CASE_ID,
		{{ ref('XfmCheckIdNull__OutCheckIdNullSorted') }}.CREATED_TIMESTAMP,
		{{ ref('XfmCheckIdNull__OutCheckIdNullSorted') }}.WIM_PROCESS_ID,
		{{ ref('XfmCheckIdNull__OutCheckIdNullSorted') }}.ORIG_ETL_D,
		{{ ref('CpyRejectOra') }}.SM_CASE_ID AS SM_CASE_ID_R,
		{{ ref('CpyRejectOra') }}.CREATED_TIMESTAMP_R,
		{{ ref('CpyRejectOra') }}.WIM_PROCESS_ID_R,
		{{ ref('CpyRejectOra') }}.ORIG_ETL_D_R
	FROM {{ ref('XfmCheckIdNull__OutCheckIdNullSorted') }}
	OUTER JOIN {{ ref('CpyRejectOra') }} ON {{ ref('XfmCheckIdNull__OutCheckIdNullSorted') }}.SM_CASE_ID = {{ ref('CpyRejectOra') }}.SM_CASE_ID
)

SELECT * FROM JoinSrcSortReject