{{ config(materialized='view', tags=['ExtHL_FEATURE_ATTR']) }}

WITH CpyRejtHlFeatureAttrRejectOra AS (
	SELECT
		HL_FEATURE_ATTR_ID,
		{{ ref('SrcRejtHlFeatureAttrRejectOra') }}.FEATURE_TERM AS FEATURE_TERM_R,
		{{ ref('SrcRejtHlFeatureAttrRejectOra') }}.FEATURE_AMOUNT AS FEATURE_AMOUNT_R,
		{{ ref('SrcRejtHlFeatureAttrRejectOra') }}.FEATURE_BALANCE AS FEATURE_BALANCE_R,
		{{ ref('SrcRejtHlFeatureAttrRejectOra') }}.FEATURE_FEE AS FEATURE_FEE_R,
		{{ ref('SrcRejtHlFeatureAttrRejectOra') }}.FEATURE_SPEC_REPAY AS FEATURE_SPEC_REPAY_R,
		{{ ref('SrcRejtHlFeatureAttrRejectOra') }}.FEATURE_EST_INT_AMT AS FEATURE_EST_INT_AMT_R,
		{{ ref('SrcRejtHlFeatureAttrRejectOra') }}.FEATURE_DATE AS FEATURE_DATE_R,
		{{ ref('SrcRejtHlFeatureAttrRejectOra') }}.FEATURE_COMMENT AS FEATURE_COMMENT_R,
		{{ ref('SrcRejtHlFeatureAttrRejectOra') }}.HL_FEATURE_CAT_ID AS HL_FEATURE_CAT_ID_R,
		{{ ref('SrcRejtHlFeatureAttrRejectOra') }}.HL_APP_PROD_ID AS HL_APP_PROD_ID_R,
		{{ ref('SrcRejtHlFeatureAttrRejectOra') }}.ORIG_ETL_D AS ORIG_ETL_D_R
	FROM {{ ref('SrcRejtHlFeatureAttrRejectOra') }}
)

SELECT * FROM CpyRejtHlFeatureAttrRejectOra