{{ config(materialized='view', tags=['XfmAppCclAppChlAppFrmExt']) }}

WITH LkpReferences AS (
	SELECT
		{{ ref('CpyRename') }}.APP_ID,
		{{ ref('CpyRename') }}.APP_APP_ID,
		{{ ref('CpyRename') }}.SBTY_CODE,
		{{ ref('CpyRename') }}.APP_APP_NO,
		{{ ref('CpyRename') }}.APP_CREATED_DATE,
		{{ ref('CpyRename') }}.APP_CREATED_BY_STAFF_NUMBER,
		{{ ref('CpyRename') }}.APP_OWNED_BY_STAFF_NUMBER,
		{{ ref('CpyRename') }}.CHNL_CAT_ID,
		{{ ref('CpyRename') }}.APP_LODGEMENT_BRANCH_ID,
		{{ ref('CpyRename') }}.APP_SM_CASE_ID,
		{{ ref('CpyRename') }}.CCL_APP_CCL_APP_ID,
		{{ ref('CpyRename') }}.CCL_APP_CAT_ID,
		{{ ref('CpyRename') }}.CCL_FORM_CAT_ID,
		{{ ref('CpyRename') }}.CCL_APP_TOT_PERSONAL_FAC_AMT,
		{{ ref('CpyRename') }}.CCL_APP_TOT_EQUIPFIN_FAC_AMT,
		{{ ref('CpyRename') }}.CCL_APP_TOT_COMMERCIAL_FAC_AMT,
		{{ ref('CpyRename') }}.CCL_APP_TOPUP_APP_ID,
		{{ ref('CpyRename') }}.CCL_APP_AF_PRIMARY_INDUSTRY_ID,
		{{ ref('CpyRename') }}.CCL_APP_AD_TUC_AMT,
		{{ ref('CpyRename') }}.CCL_APP_COMMISSION_AMT,
		{{ ref('CpyRename') }}.CCL_APP_BROKER_REFERAL_FLAG,
		{{ ref('CpyRename') }}.CHL_APP_HL_APP_ID,
		{{ ref('CpyRename') }}.CHL_APP_HL_PACKAGE_CAT_ID,
		{{ ref('CpyRename') }}.CHL_APP_LPC_OFFICE,
		{{ ref('CpyRename') }}.CHL_APP_STATUS_TRACKER_ID,
		{{ ref('CpyRename') }}.APP_FOUND_FLAG,
		{{ ref('CpyRename') }}.CCL_APP_FOUND_FLAG,
		{{ ref('CpyRename') }}.CHL_APP_FOUND_FLAG,
		{{ ref('CpyRename') }}.ORIG_ETL_D,
		{{ ref('SrcMAP_CSE_APPT_QLFYLks') }}.APPT_QLFY_C,
		{{ ref('SrcMAP_CSE_APPT_CLks') }}.APPT_C,
		{{ ref('SrcMAP_CSE_APPT_FORMLks') }}.APPT_FORM_C,
		{{ ref('SrcMAP_CSE_APPT_ORIGLks') }}.APPT_ORIG_C,
		{{ ref('TgtMAP_CSE_TU_APPT_CLks') }}.APPT_C AS APPT_C_TU,
		{{ ref('CpyRename') }}.CHL_TPB_HL_APP_ID,
		{{ ref('CpyRename') }}.CHL_TPB_SUBTYPE_CODE,
		{{ ref('CpyRename') }}.STAT_C AS REL_MANAGER_STATE_ID,
		{{ ref('CpyRename') }}.DATE_RECEIVED,
		{{ ref('CpyRename') }}.CHL_TPB_FOUND_FLAG,
		{{ ref('SrcMAP_CSE_APPT_QLFYLks_CP') }}.APPT_QLFY_C AS TPB_APPT_QLFY_C,
		{{ ref('TgtMAP_CSE_STATELks') }}.STAT_X,
		{{ ref('CpyRename') }}.ORIG_SRCE_SYST_I,
		{{ ref('TgtMAP_CSE_ORIG_SRCE_SYS_CLks') }}.ORIG_SRCE_SYST_C,
		{{ ref('TgtMAP_CSE_TPB_APPT_CLks') }}.APPT_C AS APPT_C_TPB,
		{{ ref('SrcMAP_CSE_APPT_CODE_HMTeraVw') }}.APPT_C AS APPT_C_HLM,
		{{ ref('SrcMAP_CSE_ORIG_SRCE_SYST_HMTeraVw') }}.ORIG_APPT_SRCE_SYST_C,
		{{ ref('CpyRename') }}.HL_BUSN_CHNL_CAT_I,
		{{ ref('CpyRename') }}.HLM_APP_TYPE_CAT_ID,
		{{ ref('CpyRename') }}.HLM_APP_PROD_ID,
		{{ ref('CpyRename') }}.HLM_APP_DISCHARGE_REASON_ID,
		{{ ref('CpyRename') }}.HLM_APP_CRIS_PRODUCT_ID,
		{{ ref('CpyRename') }}.HLM_APP_ACCOUNT_NUMBER,
		{{ ref('CpyRename') }}.HLM_APP_FOUND_FLAG,
		{{ ref('CpyRename') }}.CCC_APP_CC_APP_CAT_ID,
		{{ ref('CpyRename') }}.CCC_APP_FOUND_FLAG
	FROM {{ ref('CpyRename') }}
	LEFT JOIN {{ ref('SrcMAP_CSE_ORIG_SRCE_SYST_HMTeraVw') }} ON {{ ref('CpyRename') }}.HL_BUSN_CHNL_CAT_I = {{ ref('SrcMAP_CSE_ORIG_SRCE_SYST_HMTeraVw') }}.HL_BUSN_CHNL_CAT_I
	LEFT JOIN {{ ref('SrcMAP_CSE_APPT_CODE_HMTeraVw') }} ON {{ ref('CpyRename') }}.HLM_APP_TYPE_CAT_ID = {{ ref('SrcMAP_CSE_APPT_CODE_HMTeraVw') }}.HLM_APP_TYPE_CAT_ID
	LEFT JOIN {{ ref('SrcMAP_CSE_APPT_QLFYLks') }} ON 
	LEFT JOIN {{ ref('SrcMAP_CSE_APPT_CLks') }} ON 
	LEFT JOIN {{ ref('SrcMAP_CSE_APPT_FORMLks') }} ON 
	LEFT JOIN {{ ref('SrcMAP_CSE_APPT_ORIGLks') }} ON 
	LEFT JOIN {{ ref('TgtMAP_CSE_TU_APPT_CLks') }} ON 
	LEFT JOIN {{ ref('SrcMAP_CSE_APPT_QLFYLks_CP') }} ON 
	LEFT JOIN {{ ref('TgtMAP_CSE_STATELks') }} ON 
	LEFT JOIN {{ ref('TgtMAP_CSE_ORIG_SRCE_SYS_CLks') }} ON 
	LEFT JOIN {{ ref('TgtMAP_CSE_TPB_APPT_CLks') }} ON 
)

SELECT * FROM LkpReferences