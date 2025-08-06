{{ config(materialized='view', tags=['ExtAppCclAppChlApp']) }}

WITH Transformer_216 AS (
	SELECT
		APP_ID,
		{{ ref('SrcRejtRejectOra') }}.APP_APP_ID AS APP_APP_ID_R,
		{{ ref('SrcRejtRejectOra') }}.APP_SUBTYPE_CODE AS APP_SUBTYPE_CODE_R,
		{{ ref('SrcRejtRejectOra') }}.APP_APP_NO AS APP_APP_NO_R,
		{{ ref('SrcRejtRejectOra') }}.APP_CREATED_DATE AS APP_CREATED_DATE_R,
		{{ ref('SrcRejtRejectOra') }}.APP_CREATED_BY_STAFF_NUMBER AS APP_CREATED_BY_STAFF_NUMBER_R,
		{{ ref('SrcRejtRejectOra') }}.APP_OWNED_BY_STAFF_NUMBER AS APP_OWNED_BY_STAFF_NUMBER_R,
		{{ ref('SrcRejtRejectOra') }}.APP_CHANNEL_CAT_ID AS APP_CHANNEL_CAT_ID_R,
		{{ ref('SrcRejtRejectOra') }}.APP_LODGEMENT_BRANCH_ID AS APP_LODGEMENT_BRANCH_ID_R,
		{{ ref('SrcRejtRejectOra') }}.APP_SM_CASE_ID AS APP_SM_CASE_ID_R,
		{{ ref('SrcRejtRejectOra') }}.CCL_APP_CCL_APP_ID AS CCL_APP_CCL_APP_ID_R,
		{{ ref('SrcRejtRejectOra') }}.CCL_APP_CCL_APP_CAT_ID AS CCL_APP_CCL_APP_CAT_ID_R,
		{{ ref('SrcRejtRejectOra') }}.CCL_APP_CCL_FORM_CAT_ID AS CCL_APP_CCL_FORM_CAT_ID_R,
		{{ ref('SrcRejtRejectOra') }}.CCL_APP_TOT_PERSONAL_FAC_AMT AS CCL_APP_TOT_PERSONAL_FAC_AMT_R,
		{{ ref('SrcRejtRejectOra') }}.CCL_APP_TOT_EQUIPFIN_FAC_AMT AS CCL_APP_TOT_EQUIPFIN_FAC_AMT_R,
		{{ ref('SrcRejtRejectOra') }}.CCL_APP_TOT_COMMERCIAL_FAC_AMT AS CCL_APP_TOT_COMMERCIAL_FAC_AMT_R,
		{{ ref('SrcRejtRejectOra') }}.CCL_APP_TOPUP_APP_ID AS CCL_APP_TOPUP_APP_ID_R,
		{{ ref('SrcRejtRejectOra') }}.CCL_APP_AF_PRIMARY_INDUSTRY_ID AS CCL_APP_AF_PRIMARY_INDUSTRY_ID_R,
		{{ ref('SrcRejtRejectOra') }}.CCL_APP_AD_TUC_AMT AS CCL_APP_AD_TUC_AMT_R,
		{{ ref('SrcRejtRejectOra') }}.CCL_APP_COMMISSION_AMT AS CCL_APP_COMMISSION_AMT_R,
		{{ ref('SrcRejtRejectOra') }}.CCL_APP_BROKER_REFERAL_FLAG AS CCL_APP_BROKER_REFERAL_FLAG_R,
		{{ ref('SrcRejtRejectOra') }}.CHL_APP_HL_APP_ID AS CHL_APP_HL_APP_ID_R,
		{{ ref('SrcRejtRejectOra') }}.CHL_APP_HL_PACKAGE_CAT_ID AS CHL_APP_HL_PACKAGE_CAT_ID_R,
		{{ ref('SrcRejtRejectOra') }}.CHL_APP_LPC_OFFICE AS CHL_APP_LPC_OFFICE_R,
		{{ ref('SrcRejtRejectOra') }}.CHL_APP_STATUS_TRACKER_ID AS CHL_APP_STATUS_TRACKER_ID_R,
		{{ ref('SrcRejtRejectOra') }}.APP_FOUND_FLAG AS APP_FOUND_FLAG_R,
		{{ ref('SrcRejtRejectOra') }}.CCL_APP_FOUND_FLAG AS CCL_APP_FOUND_FLAG_R,
		{{ ref('SrcRejtRejectOra') }}.CHL_APP_FOUND_FLAG AS CHL_APP_FOUND_FLAG_R,
		{{ ref('SrcRejtRejectOra') }}.ORIG_ETL_D AS ORIG_ETL_D_R,
		{{ ref('SrcRejtRejectOra') }}.CHL_TPB_HL_APP_ID AS CHL_TPB_HL_APP_ID_R,
		{{ ref('SrcRejtRejectOra') }}.CHL_TPB_SUBTYPE_CODE AS CHL_TPB_SUBTYPE_CODE_R,
		{{ ref('SrcRejtRejectOra') }}.REL_MANAGER_STATE_ID AS REL_MANAGER_STATE_ID_R,
		{{ ref('SrcRejtRejectOra') }}.DATE_RECEIVED AS DATE_RECEIVED_R,
		{{ ref('SrcRejtRejectOra') }}.CHL_APP_PCD_EXT_SYS_CAT_ID AS CHL_APP_PCD_EXT_SYS_CAT_ID_R,
		{{ ref('SrcRejtRejectOra') }}.CHL_TPB_FOUND_FLAG AS CHL_TPB_FOUND_FLAG_R,
		{{ ref('SrcRejtRejectOra') }}.HLM_APP_TYPE_CAT_ID AS HLM_APP_TYPE_CAT_ID_R,
		{{ ref('SrcRejtRejectOra') }}.HLM_APP_PROD_ID AS HLM_APP_PROD_ID_R,
		{{ ref('SrcRejtRejectOra') }}.HLM_APP_DISCHARGE_REASON_ID AS HLM_APP_DISCHARGE_REASON_ID_R,
		{{ ref('SrcRejtRejectOra') }}.HLM_APP_CRIS_PRODUCT_ID AS HLM_APP_CRIS_PRODUCT_ID_R,
		{{ ref('SrcRejtRejectOra') }}.HLM_APP_ACCOUNT_NUMBER AS HLM_APP_ACCOUNT_NUMBER_R,
		{{ ref('SrcRejtRejectOra') }}.HLM_APP_FOUND_FLAG AS HLM_APP_FOUND_FLAG_R,
		{{ ref('SrcRejtRejectOra') }}.HL_BUSN_CHNL_CAT_I AS HL_BUSN_CHNL_CAT_I_R,
		{{ ref('SrcRejtRejectOra') }}.CCC_APP_CC_APP_CAT_ID AS CCC_APP_CC_APP_CAT_ID_R,
		{{ ref('SrcRejtRejectOra') }}.CCC_APP_FOUND_FLAG AS CCC_APP_FOUND_FLAG_R
	FROM {{ ref('SrcRejtRejectOra') }}
	WHERE 
)

SELECT * FROM Transformer_216