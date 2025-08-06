{{ config(materialized='view', tags=['ExtCclAppProd']) }}

WITH XfmSeparateRejectWithoutSourceAndTheRest__InRejectWithoutSourceRec AS (
	SELECT
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((XfmSeparateRejects.CCL_APP_PROD_ID)) THEN (XfmSeparateRejects.CCL_APP_PROD_ID) ELSE ""))) = 0) Then 'R' Else 'S',
		IFF(LEN(TRIM(IFF({{ ref('JoinSrcSortReject') }}.CCL_APP_PROD_ID IS NOT NULL, {{ ref('JoinSrcSortReject') }}.CCL_APP_PROD_ID, ''))) = 0, 'R', 'S') AS DeltaFlag,
		{{ ref('JoinSrcSortReject') }}.CCL_APP_PROD_ID_R AS CCL_APP_PROD_ID,
		{{ ref('JoinSrcSortReject') }}.PARENT_CCL_APP_PROD_ID_R AS PARENT_CCL_APP_PROD_ID,
		{{ ref('JoinSrcSortReject') }}.ACCOUNT_NUMBER_R AS ACCOUNT_NUMBER,
		{{ ref('JoinSrcSortReject') }}.ACTUAL_RATE_R AS ACTUAL_RATE,
		{{ ref('JoinSrcSortReject') }}.BALLOON_PAY_AMT_R AS BALLOON_PAY_AMT,
		{{ ref('JoinSrcSortReject') }}.BASIS_TYPE_CAT_ID_R AS BASIS_TYPE_CAT_ID,
		{{ ref('JoinSrcSortReject') }}.INTEREST_RATE_MARGIN_R AS INTEREST_RATE_MARGIN,
		{{ ref('JoinSrcSortReject') }}.LOAN_TERM_MTH_NUM_R AS LOAN_TERM_MTH_NUM,
		{{ ref('JoinSrcSortReject') }}.CURRENT_LIMIT_R AS CURRENT_LIMIT,
		{{ ref('JoinSrcSortReject') }}.INC_DEC_AMT_R AS INC_DEC_AMT,
		{{ ref('JoinSrcSortReject') }}.POST_APP_AMT_R AS POST_APP_AMT,
		{{ ref('JoinSrcSortReject') }}.NEW_PROD_FLAG_R AS NEW_PROD_FLAG,
		{{ ref('JoinSrcSortReject') }}.INDEX_RATE_R AS INDEX_RATE,
		{{ ref('JoinSrcSortReject') }}.INTEREST_ADV_FLAG_R AS INTEREST_ADV_FLAG,
		{{ ref('JoinSrcSortReject') }}.INTERESTONLY_TERM_MTH_NUM_R AS INTERESTONLY_TERM_MTH_NUM,
		{{ ref('JoinSrcSortReject') }}.CAPPED_TERM_MTH_NUM_R AS CAPPED_TERM_MTH_NUM,
		{{ ref('JoinSrcSortReject') }}.CAPPED_RATE_R AS CAPPED_RATE,
		{{ ref('JoinSrcSortReject') }}.CRIS_PRODUCT_ID_R AS CRIS_PRODUCT_ID,
		{{ ref('JoinSrcSortReject') }}.CCL_LOAN_PURPOSE_ID_R AS CCL_LOAN_PURPOSE_ID,
		{{ ref('JoinSrcSortReject') }}.NEW_PROD_SUBTYPE_FLAG_R AS NEW_PROD_SUBTYPE_FLAG,
		{{ ref('JoinSrcSortReject') }}.TEMP_EXCESS_MTHS_R AS TEMP_EXCESS_MTHS,
		{{ ref('JoinSrcSortReject') }}.LOAN_PURPOSE_CLASS_CODE_R AS LOAN_PURPOSE_CLASS_CODE,
		{{ ref('JoinSrcSortReject') }}.CCL_INTEREST_RATE_CAT_ID_R AS CCL_INTEREST_RATE_CAT_ID,
		{{ ref('JoinSrcSortReject') }}.CRR_R AS CRR,
		{{ ref('JoinSrcSortReject') }}.AD_TUC_INC_AMT_R AS AD_TUC_INC_AMT,
		{{ ref('JoinSrcSortReject') }}.CCL_RATE_TYPE_CAT_ID_R AS CCL_RATE_TYPE_CAT_ID,
		{{ ref('JoinSrcSortReject') }}.LINK_CCL_APP_ID_R AS LINK_CCL_APP_ID,
		{{ ref('JoinSrcSortReject') }}.LINK_CCL_APP_PROD_ID_R AS LINK_CCL_APP_PROD_ID,
		{{ ref('JoinSrcSortReject') }}.CHL_LMI_AMT_R AS CHL_LMI_AMT,
		{{ ref('JoinSrcSortReject') }}.USED_IN_EXPOSURE_VIEW_FLAG_R AS USED_IN_EXPOSURE_VIEW_FLAG,
		{{ ref('JoinSrcSortReject') }}.CCL_APP_BROKER_CIF_CODE_R AS CCL_APP_BROKER_CIF_CODE,
		{{ ref('JoinSrcSortReject') }}.APP_ID_R AS APP_ID,
		{{ ref('JoinSrcSortReject') }}.FIXED_INT_START_DATE_R AS FIXED_INT_START_DATE,
		{{ ref('JoinSrcSortReject') }}.CHILD_PRODUCT_LEVEL_CAT_ID_R AS CHILD_PRODUCT_LEVEL_CAT_ID,
		{{ ref('JoinSrcSortReject') }}.PARENT_PRODUCT_LEVEL_CAT_ID_R AS PARENT_PRODUCT_LEVEL_CAT_ID,
		{{ ref('JoinSrcSortReject') }}.ORIG_ETL_D_R AS ORIG_ETL_D
	FROM {{ ref('JoinSrcSortReject') }}
	WHERE DeltaFlag = 'R'
)

SELECT * FROM XfmSeparateRejectWithoutSourceAndTheRest__InRejectWithoutSourceRec