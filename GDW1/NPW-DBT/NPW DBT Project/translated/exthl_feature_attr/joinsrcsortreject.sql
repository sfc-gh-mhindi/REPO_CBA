{{ config(materialized='view', tags=['ExtHL_FEATURE_ATTR']) }}

WITH JoinSrcSortReject AS (
	SELECT
		{{ ref('XfmCheckHlFeatureAttrIdNulls__OutCheckHlFeatureAttrIdNullsSorted') }}.HL_FEATURE_ATTR_ID,
		{{ ref('XfmCheckHlFeatureAttrIdNulls__OutCheckHlFeatureAttrIdNullsSorted') }}.HL_FEATURE_TERM,
		{{ ref('XfmCheckHlFeatureAttrIdNulls__OutCheckHlFeatureAttrIdNullsSorted') }}.HL_FEATURE_AMOUNT,
		{{ ref('XfmCheckHlFeatureAttrIdNulls__OutCheckHlFeatureAttrIdNullsSorted') }}.HL_FEATURE_BALANCE,
		{{ ref('XfmCheckHlFeatureAttrIdNulls__OutCheckHlFeatureAttrIdNullsSorted') }}.HL_FEATURE_FEE,
		{{ ref('XfmCheckHlFeatureAttrIdNulls__OutCheckHlFeatureAttrIdNullsSorted') }}.HL_FEATURE_SPEC_REPAY,
		{{ ref('XfmCheckHlFeatureAttrIdNulls__OutCheckHlFeatureAttrIdNullsSorted') }}.HL_FEATURE_EST_INT_AMT,
		{{ ref('XfmCheckHlFeatureAttrIdNulls__OutCheckHlFeatureAttrIdNullsSorted') }}.HL_FEATURE_DATE,
		{{ ref('XfmCheckHlFeatureAttrIdNulls__OutCheckHlFeatureAttrIdNullsSorted') }}.HL_FEATURE_COMMENT,
		{{ ref('XfmCheckHlFeatureAttrIdNulls__OutCheckHlFeatureAttrIdNullsSorted') }}.HL_FEATURE_CAT_ID,
		{{ ref('XfmCheckHlFeatureAttrIdNulls__OutCheckHlFeatureAttrIdNullsSorted') }}.HL_APP_PROD_ID,
		{{ ref('CpyRejtHlFeatureAttrRejectOra') }}.HL_FEATURE_ATTR_ID AS HL_FEATURE_ATTR_ID_R,
		{{ ref('CpyRejtHlFeatureAttrRejectOra') }}.FEATURE_TERM_R,
		{{ ref('CpyRejtHlFeatureAttrRejectOra') }}.FEATURE_AMOUNT_R,
		{{ ref('CpyRejtHlFeatureAttrRejectOra') }}.FEATURE_BALANCE_R,
		{{ ref('CpyRejtHlFeatureAttrRejectOra') }}.FEATURE_FEE_R,
		{{ ref('CpyRejtHlFeatureAttrRejectOra') }}.FEATURE_SPEC_REPAY_R,
		{{ ref('CpyRejtHlFeatureAttrRejectOra') }}.FEATURE_EST_INT_AMT_R,
		{{ ref('CpyRejtHlFeatureAttrRejectOra') }}.FEATURE_DATE_R,
		{{ ref('CpyRejtHlFeatureAttrRejectOra') }}.FEATURE_COMMENT_R,
		{{ ref('CpyRejtHlFeatureAttrRejectOra') }}.HL_FEATURE_CAT_ID_R,
		{{ ref('CpyRejtHlFeatureAttrRejectOra') }}.HL_APP_PROD_ID_R,
		{{ ref('CpyRejtHlFeatureAttrRejectOra') }}.ORIG_ETL_D_R
	FROM {{ ref('XfmCheckHlFeatureAttrIdNulls__OutCheckHlFeatureAttrIdNullsSorted') }}
	OUTER JOIN {{ ref('CpyRejtHlFeatureAttrRejectOra') }} ON {{ ref('XfmCheckHlFeatureAttrIdNulls__OutCheckHlFeatureAttrIdNullsSorted') }}.HL_FEATURE_ATTR_ID = {{ ref('CpyRejtHlFeatureAttrRejectOra') }}.HL_FEATURE_ATTR_ID
)

SELECT * FROM JoinSrcSortReject