{{ config(materialized='view', tags=['XfmCclAppProdFrmExt2']) }}

WITH LkpReferences AS (
	SELECT
		{{ ref('XfmConcatNewProdSubTypeFlag') }}.APP_PROD_ID,
		{{ ref('XfmConcatNewProdSubTypeFlag') }}.PARENT_CCL_APP_PROD_ID,
		{{ ref('XfmConcatNewProdSubTypeFlag') }}.ACCOUNT_NUMBER,
		{{ ref('XfmConcatNewProdSubTypeFlag') }}.ACTUAL_RATE,
		{{ ref('XfmConcatNewProdSubTypeFlag') }}.BALLOON_PAY_AMT,
		{{ ref('XfmConcatNewProdSubTypeFlag') }}.BASIS_TYPE_CAT_ID,
		{{ ref('XfmConcatNewProdSubTypeFlag') }}.INTEREST_RATE_MARGIN,
		{{ ref('XfmConcatNewProdSubTypeFlag') }}.LOAN_TERM_MTH_NUM,
		{{ ref('XfmConcatNewProdSubTypeFlag') }}.CURRENT_LIMIT,
		{{ ref('XfmConcatNewProdSubTypeFlag') }}.INC_DEC_AMT,
		{{ ref('XfmConcatNewProdSubTypeFlag') }}.POST_APP_AMT,
		{{ ref('XfmConcatNewProdSubTypeFlag') }}.NEW_PROD_FLAG,
		{{ ref('XfmConcatNewProdSubTypeFlag') }}.INDEX_RATE,
		{{ ref('XfmConcatNewProdSubTypeFlag') }}.INTEREST_ADV_FLAG,
		{{ ref('XfmConcatNewProdSubTypeFlag') }}.INTERESTONLY_TERM_MTH_NUM,
		{{ ref('XfmConcatNewProdSubTypeFlag') }}.CAPPED_TERM_MTH_NUM,
		{{ ref('XfmConcatNewProdSubTypeFlag') }}.CAPPED_RATE,
		{{ ref('XfmConcatNewProdSubTypeFlag') }}.CRIS_PRODUCT_ID,
		{{ ref('XfmConcatNewProdSubTypeFlag') }}.CCL_LOAN_PURPOSE_ID,
		{{ ref('XfmConcatNewProdSubTypeFlag') }}.NEW_PROD_SUBTYPE_FLAG,
		{{ ref('XfmConcatNewProdSubTypeFlag') }}.TEMP_EXCESS_MTHS,
		{{ ref('XfmConcatNewProdSubTypeFlag') }}.LOAN_PURPOSE_CLASS_CODE,
		{{ ref('XfmConcatNewProdSubTypeFlag') }}.CCL_INTEREST_RATE_CAT_ID,
		{{ ref('XfmConcatNewProdSubTypeFlag') }}.CRR,
		{{ ref('XfmConcatNewProdSubTypeFlag') }}.AD_TUC_INC_AMT,
		{{ ref('XfmConcatNewProdSubTypeFlag') }}.CCL_RATE_TYPE_CAT_ID,
		{{ ref('XfmConcatNewProdSubTypeFlag') }}.LINK_CCL_APP_ID,
		{{ ref('XfmConcatNewProdSubTypeFlag') }}.LINK_CCL_APP_PROD_ID,
		{{ ref('XfmConcatNewProdSubTypeFlag') }}.CHL_LMI_AMT,
		{{ ref('XfmConcatNewProdSubTypeFlag') }}.USED_IN_EXPOSURE_VIEW_FLAG,
		{{ ref('XfmConcatNewProdSubTypeFlag') }}.CCL_APP_BROKER_CIF_CODE,
		{{ ref('XfmConcatNewProdSubTypeFlag') }}.APP_ID,
		{{ ref('XfmConcatNewProdSubTypeFlag') }}.FIXED_INT_START_DATE,
		{{ ref('XfmConcatNewProdSubTypeFlag') }}.CHILD_PRODUCT_LEVEL_CAT_ID,
		{{ ref('XfmConcatNewProdSubTypeFlag') }}.PARENT_PRODUCT_LEVEL_CAT_ID,
		{{ ref('XfmConcatNewProdSubTypeFlag') }}.ORIG_ETL_D,
		{{ ref('RD_FEAT_I_1') }}.FEAT_I_1,
		{{ ref('RD_FEAT_I_2') }}.FEAT_I_2,
		{{ ref('RD_FEAT_I_3') }}.FEAT_I_3,
		{{ ref('RD_FEAT_I_4') }}.FEAT_I_4,
		{{ ref('RD_FEAT_I_5') }}.FEAT_I_5,
		{{ ref('RD_FEAT_I_6') }}.FEAT_I_6
	FROM {{ ref('XfmConcatNewProdSubTypeFlag') }}
	LEFT JOIN {{ ref('RD_FEAT_I_1') }} ON {{ ref('XfmConcatNewProdSubTypeFlag') }}.CL_FEAT_RATE_LITERAL = {{ ref('RD_FEAT_I_1') }}.MAP_TYPE_C
	AND {{ ref('XfmConcatNewProdSubTypeFlag') }}.CCL_INTEREST_RATE_CAT_ID = {{ ref('RD_FEAT_I_1') }}.SRCE_NUMC_1_C
	AND {{ ref('XfmConcatNewProdSubTypeFlag') }}.CCL_RATE_TYPE_CAT_ID = {{ ref('RD_FEAT_I_1') }}.SRCE_NUMC_2_C
	LEFT JOIN {{ ref('RD_FEAT_I_2') }} ON {{ ref('XfmConcatNewProdSubTypeFlag') }}.CL_FEAT_TERM_LITERAL = {{ ref('RD_FEAT_I_2') }}.MAP_TYPE_C
	AND {{ ref('XfmConcatNewProdSubTypeFlag') }}.ORIG_LITERAL = {{ ref('RD_FEAT_I_2') }}.SRCE_CHAR_1_C
	LEFT JOIN {{ ref('RD_FEAT_I_3') }} ON {{ ref('XfmConcatNewProdSubTypeFlag') }}.CL_FEAT_TERM_LITERAL = {{ ref('RD_FEAT_I_3') }}.MAP_TYPE_C
	AND {{ ref('XfmConcatNewProdSubTypeFlag') }}.IOLT_LITERAL = {{ ref('RD_FEAT_I_3') }}.SRCE_CHAR_1_C
	LEFT JOIN {{ ref('RD_FEAT_I_4') }} ON {{ ref('XfmConcatNewProdSubTypeFlag') }}.CL_FEAT_SRAT_LITERAL = {{ ref('RD_FEAT_I_4') }}.MAP_TYPE_C
	AND {{ ref('XfmConcatNewProdSubTypeFlag') }}.CCL_INTEREST_RATE_CAT_ID = {{ ref('RD_FEAT_I_4') }}.SRCE_NUMC_1_C
	AND {{ ref('XfmConcatNewProdSubTypeFlag') }}.NA_LITERAL = {{ ref('RD_FEAT_I_4') }}.SRCE_CHAR_2_C
	LEFT JOIN {{ ref('RD_FEAT_I_5') }} ON {{ ref('XfmConcatNewProdSubTypeFlag') }}.CL_FEAT_TERM_LITERAL = {{ ref('RD_FEAT_I_5') }}.MAP_TYPE_C
	AND {{ ref('XfmConcatNewProdSubTypeFlag') }}.CAPT_LITERAL = {{ ref('RD_FEAT_I_5') }}.SRCE_CHAR_1_C
	LEFT JOIN {{ ref('RD_FEAT_I_6') }} ON {{ ref('XfmConcatNewProdSubTypeFlag') }}.CL_FEAT_TERM_LITERAL = {{ ref('RD_FEAT_I_6') }}.MAP_TYPE_C
	AND {{ ref('XfmConcatNewProdSubTypeFlag') }}.TMP_NEW_PROD_SUBTYPE_FLAG = {{ ref('RD_FEAT_I_6') }}.SRCE_CHAR_1_C
)

SELECT * FROM LkpReferences