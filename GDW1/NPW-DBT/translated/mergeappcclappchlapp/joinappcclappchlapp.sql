{{ config(materialized='view', tags=['MergeAppCclAppChlApp']) }}

WITH JoinAppCclAppChlApp AS (
	SELECT
		{{ ref('Identify_APP_ID_AppCclApp') }}.APP_ID AS APP_CCL_APP_APP_ID,
		{{ ref('Identify_APP_ID_AppCclApp') }}.APP_APP_ID,
		{{ ref('Identify_APP_ID_AppCclApp') }}.APP_SUBTYPE_CODE,
		{{ ref('Identify_APP_ID_AppCclApp') }}.APP_APP_NO,
		{{ ref('Identify_APP_ID_AppCclApp') }}.APP_CREATED_DATE,
		{{ ref('Identify_APP_ID_AppCclApp') }}.APP_CREATED_BY_STAFF_NUMBER,
		{{ ref('Identify_APP_ID_AppCclApp') }}.APP_OWNED_BY_STAFF_NUMBER,
		{{ ref('Identify_APP_ID_AppCclApp') }}.APP_CHANNEL_CAT_ID,
		{{ ref('Identify_APP_ID_AppCclApp') }}.APP_LODGEMENT_BRANCH_ID,
		{{ ref('Identify_APP_ID_AppCclApp') }}.APP_SM_CASE_ID,
		{{ ref('Identify_APP_ID_AppCclApp') }}.CCL_APP_CCL_APP_ID,
		{{ ref('Identify_APP_ID_AppCclApp') }}.CCL_APP_CCL_APP_CAT_ID,
		{{ ref('Identify_APP_ID_AppCclApp') }}.CCL_APP_CCL_FORM_CAT_ID,
		{{ ref('Identify_APP_ID_AppCclApp') }}.CCL_APP_TOT_PERSONAL_FAC_AMT,
		{{ ref('Identify_APP_ID_AppCclApp') }}.CCL_APP_TOT_EQUIPFIN_FAC_AMT,
		{{ ref('Identify_APP_ID_AppCclApp') }}.CCL_APP_TOT_COMMERCIAL_FAC_AMT,
		{{ ref('Identify_APP_ID_AppCclApp') }}.CCL_APP_TOPUP_APP_ID,
		{{ ref('Identify_APP_ID_AppCclApp') }}.CCL_APP_AF_PRIMARY_INDUSTRY_ID,
		{{ ref('Identify_APP_ID_AppCclApp') }}.CCL_APP_AD_TUC_AMT,
		{{ ref('Identify_APP_ID_AppCclApp') }}.CCL_APP_COMMISSION_AMT,
		{{ ref('Identify_APP_ID_AppCclApp') }}.CCL_APP_BROKER_REFERAL_FLAG,
		{{ ref('CgAdd_Flag3') }}.APP_ID AS CHL_APP_HL_APP_ID,
		{{ ref('CgAdd_Flag3') }}.CHL_APP_HL_PACKAGE_CAT_ID,
		{{ ref('CgAdd_Flag3') }}.CHL_APP_LPC_OFFICE,
		{{ ref('CgAdd_Flag3') }}.CHL_APP_STATUS_TRACKER_ID,
		{{ ref('CgAdd_Flag3') }}.CHL_APP_PCD_EXTERNAL_SYS_CAT_ID,
		{{ ref('Identify_APP_ID_AppCclApp') }}.APP_FOUND_FLAG,
		{{ ref('Identify_APP_ID_AppCclApp') }}.CCL_APP_FOUND_FLAG,
		{{ ref('CgAdd_Flag3') }}.CHL_APP_FOUND_FLAG
	FROM {{ ref('Identify_APP_ID_AppCclApp') }}
	OUTER JOIN {{ ref('CgAdd_Flag3') }} ON {{ ref('Identify_APP_ID_AppCclApp') }}.APP_ID = {{ ref('CgAdd_Flag3') }}.APP_ID
)

SELECT * FROM JoinAppCclAppChlApp