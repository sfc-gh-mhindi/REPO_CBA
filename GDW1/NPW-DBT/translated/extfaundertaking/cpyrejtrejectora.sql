{{ config(materialized='view', tags=['ExtFaUndertaking']) }}

WITH CpyRejtRejectOra AS (
	SELECT
		FA_UNDERTAKING_ID,
		{{ ref('SrcRejtRejectOra') }}.PLANNING_GROUP_NAME AS PLANNING_GROUP_NAME_R,
		{{ ref('SrcRejtRejectOra') }}.COIN_ADVICE_GROUP_ID AS COIN_ADVICE_GROUP_ID_R,
		{{ ref('SrcRejtRejectOra') }}.ADVICE_GROUP_CORRELATION_ID AS ADVICE_GROUP_CORRELATION_ID_R,
		{{ ref('SrcRejtRejectOra') }}.CREATED_DATE AS CREATED_DATE_R,
		{{ ref('SrcRejtRejectOra') }}.CREATED_BY_STAFF_NUMBER AS CREATED_BY_STAFF_NUMBER_R,
		{{ ref('SrcRejtRejectOra') }}.SM_CASE_ID AS SM_CASE_ID_R,
		{{ ref('SrcRejtRejectOra') }}.ORIG_ETL_D AS ORIG_ETL_D_R
	FROM {{ ref('SrcRejtRejectOra') }}
)

SELECT * FROM CpyRejtRejectOra