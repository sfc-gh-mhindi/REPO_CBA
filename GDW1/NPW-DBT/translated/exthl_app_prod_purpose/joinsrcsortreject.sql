{{ config(materialized='view', tags=['ExtHL_APP_PROD_PURPOSE']) }}

WITH JoinSrcSortReject AS (
	SELECT
		{{ ref('XfmCheckHlAppProdPurposeIdNulls__OutCheckHlAppProdPurposeIdNullsSorted') }}.HL_APP_PROD_PURPOSE_ID,
		{{ ref('XfmCheckHlAppProdPurposeIdNulls__OutCheckHlAppProdPurposeIdNullsSorted') }}.HL_APP_PROD_ID,
		{{ ref('XfmCheckHlAppProdPurposeIdNulls__OutCheckHlAppProdPurposeIdNullsSorted') }}.HL_LOAN_PURPOSE_CAT_ID,
		{{ ref('XfmCheckHlAppProdPurposeIdNulls__OutCheckHlAppProdPurposeIdNullsSorted') }}.AMOUNT,
		{{ ref('XfmCheckHlAppProdPurposeIdNulls__OutCheckHlAppProdPurposeIdNullsSorted') }}.MAIN_PURPOSE,
		{{ ref('CpyRejtHlAppProdPurposeRejectOra') }}.HL_APP_PROD_PURPOSE_ID AS HL_APP_PROD_PURPOSE_ID_R,
		{{ ref('CpyRejtHlAppProdPurposeRejectOra') }}.HL_APP_PROD_ID_R,
		{{ ref('CpyRejtHlAppProdPurposeRejectOra') }}.HL_LOAN_PURPOSE_CAT_ID_R,
		{{ ref('CpyRejtHlAppProdPurposeRejectOra') }}.AMOUNT_R,
		{{ ref('CpyRejtHlAppProdPurposeRejectOra') }}.MAIN_PURPOSE_R,
		{{ ref('CpyRejtHlAppProdPurposeRejectOra') }}.ORIG_ETL_D_R
	FROM {{ ref('XfmCheckHlAppProdPurposeIdNulls__OutCheckHlAppProdPurposeIdNullsSorted') }}
	OUTER JOIN {{ ref('CpyRejtHlAppProdPurposeRejectOra') }} ON {{ ref('XfmCheckHlAppProdPurposeIdNulls__OutCheckHlAppProdPurposeIdNullsSorted') }}.HL_APP_PROD_PURPOSE_ID = {{ ref('CpyRejtHlAppProdPurposeRejectOra') }}.HL_APP_PROD_PURPOSE_ID
)

SELECT * FROM JoinSrcSortReject