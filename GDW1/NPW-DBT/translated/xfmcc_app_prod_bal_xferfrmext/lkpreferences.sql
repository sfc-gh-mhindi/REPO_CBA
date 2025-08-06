{{ config(materialized='view', tags=['XfmCC_APP_PROD_BAL_XFERFrmExt']) }}

WITH LkpReferences AS (
	SELECT
		{{ ref('CpyRename') }}.CC_APP_PROD_BAL_XFER_ID,
		{{ ref('CpyRename') }}.BAL_XFER_OPTION_CAT_ID,
		{{ ref('CpyRename') }}.XFER_AMT,
		{{ ref('CpyRename') }}.BAL_XFER_INSTITUTION_CAT_ID,
		{{ ref('CpyRename') }}.CC_APP_PROD_ID,
		{{ ref('CpyRename') }}.CC_APP_ID,
		{{ ref('CpyRename') }}.ORIG_ETL_D,
		{{ ref('SrcMAP_CSE_TRNF_OPTNLks') }}.TRNF_OPTN_C,
		{{ ref('SrcMAP_CSE_APPT_CMPELks') }}.CMPE_I
	FROM {{ ref('CpyRename') }}
	LEFT JOIN {{ ref('SrcMAP_CSE_TRNF_OPTNLks') }} ON 
	LEFT JOIN {{ ref('SrcMAP_CSE_APPT_CMPELks') }} ON 
)

SELECT * FROM LkpReferences