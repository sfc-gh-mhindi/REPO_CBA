{{ config(materialized='view', tags=['XfmCclAppProdFrmExt3']) }}

WITH LkpReferences AS (
	SELECT
		{{ ref('CpyRename') }}.CCL_APP_PROD_ID,
		{{ ref('CpyRename') }}.PARENT_CCL_APP_PROD_ID,
		{{ ref('CpyRename') }}.ACCOUNT_NUMBER,
		{{ ref('CpyRename') }}.ACTUAL_RATE,
		{{ ref('CpyRename') }}.BALLOON_PAY_AMT,
		{{ ref('CpyRename') }}.BASIS_TYPE_CAT_ID,
		{{ ref('CpyRename') }}.INTEREST_RATE_MARGIN,
		{{ ref('CpyRename') }}.LOAN_TERM_MTH_NUM,
		{{ ref('CpyRename') }}.CURRENT_LIMIT,
		{{ ref('CpyRename') }}.INC_DEC_AMT,
		{{ ref('CpyRename') }}.POST_APP_AMT,
		{{ ref('CpyRename') }}.NEW_PROD_FLAG,
		{{ ref('CpyRename') }}.INDEX_RATE,
		{{ ref('CpyRename') }}.INTEREST_ADV_FLAG,
		{{ ref('CpyRename') }}.INTERESTONLY_TERM_MTH_NUM,
		{{ ref('CpyRename') }}.CAPPED_TERM_MTH_NUM,
		{{ ref('CpyRename') }}.CAPPED_RATE,
		{{ ref('CpyRename') }}.CRIS_PDCT_CAT_ID,
		{{ ref('CpyRename') }}.CCL_LOAN_PURPOSE_CAT_ID,
		{{ ref('CpyRename') }}.NEW_PROD_SUBTYPE_FLAG,
		{{ ref('CpyRename') }}.TEMP_EXCESS_MTHS,
		{{ ref('CpyRename') }}.LOAN_PURPOSE_CLASS_CODE,
		{{ ref('CpyRename') }}.CCL_INTEREST_RATE_CAT_ID,
		{{ ref('CpyRename') }}.CRR,
		{{ ref('CpyRename') }}.AD_TUC_INC_AMT,
		{{ ref('CpyRename') }}.CCL_RATE_TYPE_CAT_ID,
		{{ ref('CpyRename') }}.LINK_CCL_APP_ID,
		{{ ref('CpyRename') }}.LINK_CCL_APP_PROD_ID,
		{{ ref('CpyRename') }}.CHL_LMI_AMT,
		{{ ref('CpyRename') }}.USED_IN_EXPOSURE_VIEW_FLAG,
		{{ ref('CpyRename') }}.CCL_APP_BROKER_CIF_CODE,
		{{ ref('CpyRename') }}.APP_ID,
		{{ ref('CpyRename') }}.FIXED_INT_START_DATE,
		{{ ref('CpyRename') }}.CHILD_PRODUCT_LEVEL_CAT_ID,
		{{ ref('CpyRename') }}.PARENT_PRODUCT_LEVEL_CAT_ID,
		{{ ref('CpyRename') }}.ORIG_ETL_D,
		{{ ref('SrcMAP_CSE_PDCT_REL_CLLks') }}.REL_C,
		{{ ref('SrcMAP_CMS_PDCTLks') }}.ACCT_I_PRFX,
		{{ ref('SrcMAP_CSE_APPT_PURP_CLLks') }}.PURP_TYPE_C,
		{{ ref('SrcMAP_CSE_APPT_PURP_CLAS_CLLks') }}.PURP_CLAS_C
	FROM {{ ref('CpyRename') }}
	LEFT JOIN {{ ref('SrcMAP_CSE_PDCT_REL_CLLks') }} ON 
	LEFT JOIN {{ ref('SrcMAP_CMS_PDCTLks') }} ON 
	LEFT JOIN {{ ref('SrcMAP_CSE_APPT_PURP_CLLks') }} ON 
	LEFT JOIN {{ ref('SrcMAP_CSE_APPT_PURP_CLAS_CLLks') }} ON 
)

SELECT * FROM LkpReferences