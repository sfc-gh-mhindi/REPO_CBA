{{ config(materialized='view', tags=['ExtCclAppProd']) }}

WITH CpyRejtRejectOra AS (
	SELECT
		CCL_APP_PROD_ID,
		{{ ref('SrcRejtRejectOra') }}.PARENT_CCL_APP_PROD_ID AS PARENT_CCL_APP_PROD_ID_R,
		{{ ref('SrcRejtRejectOra') }}.ACCOUNT_NUMBER AS ACCOUNT_NUMBER_R,
		{{ ref('SrcRejtRejectOra') }}.ACTUAL_RATE AS ACTUAL_RATE_R,
		{{ ref('SrcRejtRejectOra') }}.BALLOON_PAY_AMT AS BALLOON_PAY_AMT_R,
		{{ ref('SrcRejtRejectOra') }}.BASIS_TYPE_CAT_ID AS BASIS_TYPE_CAT_ID_R,
		{{ ref('SrcRejtRejectOra') }}.INTEREST_RATE_MARGIN AS INTEREST_RATE_MARGIN_R,
		{{ ref('SrcRejtRejectOra') }}.LOAN_TERM_MTH_NUM AS LOAN_TERM_MTH_NUM_R,
		{{ ref('SrcRejtRejectOra') }}.CURRENT_LIMIT AS CURRENT_LIMIT_R,
		{{ ref('SrcRejtRejectOra') }}.INC_DEC_AMT AS INC_DEC_AMT_R,
		{{ ref('SrcRejtRejectOra') }}.POST_APP_AMT AS POST_APP_AMT_R,
		{{ ref('SrcRejtRejectOra') }}.NEW_PROD_FLAG AS NEW_PROD_FLAG_R,
		{{ ref('SrcRejtRejectOra') }}.INDEX_RATE AS INDEX_RATE_R,
		{{ ref('SrcRejtRejectOra') }}.INTEREST_ADV_FLAG AS INTEREST_ADV_FLAG_R,
		{{ ref('SrcRejtRejectOra') }}.INTERESTONLY_TERM_MTH_NUM AS INTERESTONLY_TERM_MTH_NUM_R,
		{{ ref('SrcRejtRejectOra') }}.CAPPED_TERM_MTH_NUM AS CAPPED_TERM_MTH_NUM_R,
		{{ ref('SrcRejtRejectOra') }}.CAPPED_RATE AS CAPPED_RATE_R,
		{{ ref('SrcRejtRejectOra') }}.CRIS_PRODUCT_ID AS CRIS_PRODUCT_ID_R,
		{{ ref('SrcRejtRejectOra') }}.CCL_LOAN_PURPOSE_ID AS CCL_LOAN_PURPOSE_ID_R,
		{{ ref('SrcRejtRejectOra') }}.NEW_PROD_SUBTYPE_FLAG AS NEW_PROD_SUBTYPE_FLAG_R,
		{{ ref('SrcRejtRejectOra') }}.TEMP_EXCESS_MTHS AS TEMP_EXCESS_MTHS_R,
		{{ ref('SrcRejtRejectOra') }}.LOAN_PURPOSE_CLASS_CODE AS LOAN_PURPOSE_CLASS_CODE_R,
		{{ ref('SrcRejtRejectOra') }}.CCL_INTEREST_RATE_CAT_ID AS CCL_INTEREST_RATE_CAT_ID_R,
		{{ ref('SrcRejtRejectOra') }}.CRR AS CRR_R,
		{{ ref('SrcRejtRejectOra') }}.AD_TUC_INC_AMT AS AD_TUC_INC_AMT_R,
		{{ ref('SrcRejtRejectOra') }}.CCL_RATE_TYPE_CAT_ID AS CCL_RATE_TYPE_CAT_ID_R,
		{{ ref('SrcRejtRejectOra') }}.LINK_CCL_APP_ID AS LINK_CCL_APP_ID_R,
		{{ ref('SrcRejtRejectOra') }}.LINK_CCL_APP_PROD_ID AS LINK_CCL_APP_PROD_ID_R,
		{{ ref('SrcRejtRejectOra') }}.CHL_LMI_AMT AS CHL_LMI_AMT_R,
		{{ ref('SrcRejtRejectOra') }}.USED_IN_EXPOSURE_VIEW_FLAG AS USED_IN_EXPOSURE_VIEW_FLAG_R,
		{{ ref('SrcRejtRejectOra') }}.CCL_APP_BROKER_CIF_CODE AS CCL_APP_BROKER_CIF_CODE_R,
		{{ ref('SrcRejtRejectOra') }}.APP_ID AS APP_ID_R,
		{{ ref('SrcRejtRejectOra') }}.FIXED_INT_START_DATE AS FIXED_INT_START_DATE_R,
		{{ ref('SrcRejtRejectOra') }}.CHILD_PRODUCT_LEVEL_CAT_ID AS CHILD_PRODUCT_LEVEL_CAT_ID_R,
		{{ ref('SrcRejtRejectOra') }}.PARENT_PRODUCT_LEVEL_CAT_ID AS PARENT_PRODUCT_LEVEL_CAT_ID_R,
		{{ ref('SrcRejtRejectOra') }}.ORIG_ETL_D AS ORIG_ETL_D_R
	FROM {{ ref('SrcRejtRejectOra') }}
)

SELECT * FROM CpyRejtRejectOra