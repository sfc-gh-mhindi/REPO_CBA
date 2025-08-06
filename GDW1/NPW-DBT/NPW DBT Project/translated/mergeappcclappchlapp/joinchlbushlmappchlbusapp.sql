{{ config(materialized='view', tags=['MergeAppCclAppChlApp']) }}

WITH JoinChlBusHlmAppCHlBusApp AS (
	SELECT
		{{ ref('Identify_APP') }}.APP_ID,
		{{ ref('CgAdd_Flag5') }}.APP_ID AS rightRec_APP_ID,
		{{ ref('CgAdd_Flag5') }}.HLM_APP_TYPE_CAT_ID,
		{{ ref('CgAdd_Flag5') }}.HLM_APP_PROD_ID,
		{{ ref('CgAdd_Flag5') }}.HLM_APP_DISCHARGE_REASON_ID,
		{{ ref('CgAdd_Flag5') }}.HLM_APP_CRIS_PRODUCT_ID,
		{{ ref('CgAdd_Flag5') }}.HLM_APP_ACCOUNT_NUMBER,
		{{ ref('CgAdd_Flag5') }}.HLM_APP_FOUND_FLAG,
		{{ ref('Identify_APP') }}.APP_APP_ID,
		{{ ref('Identify_APP') }}.APP_SUBTYPE_CODE,
		{{ ref('Identify_APP') }}.APP_APP_NO,
		{{ ref('Identify_APP') }}.APP_CREATED_DATE,
		{{ ref('Identify_APP') }}.APP_CREATED_BY_STAFF_NUMBER,
		{{ ref('Identify_APP') }}.APP_OWNED_BY_STAFF_NUMBER,
		{{ ref('Identify_APP') }}.APP_CHANNEL_CAT_ID,
		{{ ref('Identify_APP') }}.APP_LODGEMENT_BRANCH_ID,
		{{ ref('Identify_APP') }}.APP_SM_CASE_ID,
		{{ ref('Identify_APP') }}.CCL_APP_CCL_APP_ID,
		{{ ref('Identify_APP') }}.CCL_APP_CCL_APP_CAT_ID,
		{{ ref('Identify_APP') }}.CCL_APP_CCL_FORM_CAT_ID,
		{{ ref('Identify_APP') }}.CCL_APP_TOT_PERSONAL_FAC_AMT,
		{{ ref('Identify_APP') }}.CCL_APP_TOT_EQUIPFIN_FAC_AMT,
		{{ ref('Identify_APP') }}.CCL_APP_TOT_COMMERCIAL_FAC_AMT,
		{{ ref('Identify_APP') }}.CCL_APP_TOPUP_APP_ID,
		{{ ref('Identify_APP') }}.CCL_APP_AF_PRIMARY_INDUSTRY_ID,
		{{ ref('Identify_APP') }}.CCL_APP_AD_TUC_AMT,
		{{ ref('Identify_APP') }}.CCL_APP_COMMISSION_AMT,
		{{ ref('Identify_APP') }}.CCL_APP_BROKER_REFERAL_FLAG,
		{{ ref('Identify_APP') }}.CHL_APP_HL_APP_ID,
		{{ ref('Identify_APP') }}.CHL_APP_HL_PACKAGE_CAT_ID,
		{{ ref('Identify_APP') }}.CHL_APP_LPC_OFFICE,
		{{ ref('Identify_APP') }}.CHL_APP_STATUS_TRACKER_ID,
		{{ ref('Identify_APP') }}.CHL_APP_PCD_EXTERNAL_SYS_CAT_ID,
		{{ ref('Identify_APP') }}.APP_FOUND_FLAG,
		{{ ref('Identify_APP') }}.CCL_APP_FOUND_FLAG,
		{{ ref('Identify_APP') }}.CHL_APP_FOUND_FLAG,
		{{ ref('Identify_APP') }}.CHL_TPB_HL_APP_ID,
		{{ ref('Identify_APP') }}.CHL_TPB_SUBTYPE_CODE,
		{{ ref('Identify_APP') }}.REL_MANAGER_STATE_ID,
		{{ ref('Identify_APP') }}.DATE_RECEIVED,
		{{ ref('Identify_APP') }}.CHL_TPB_FOUND_FLAG,
		{{ ref('Identify_APP') }}.HL_BUSN_CHNL_CAT_I
	FROM {{ ref('Identify_APP') }}
	OUTER JOIN {{ ref('CgAdd_Flag5') }} ON {{ ref('Identify_APP') }}.APP_ID = {{ ref('CgAdd_Flag5') }}.APP_ID
)

SELECT * FROM JoinChlBusHlmAppCHlBusApp