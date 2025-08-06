{{ config(materialized='view', tags=['MergeAppCclAppChlApp']) }}

WITH JoinAppCclApp AS (
	SELECT
		{{ ref('CgAdd_Flag1') }}.APP_ID AS APP_APP_ID,
		{{ ref('CgAdd_Flag1') }}.APP_SUBTYPE_CODE,
		{{ ref('CgAdd_Flag1') }}.APP_APP_NO,
		{{ ref('CgAdd_Flag1') }}.APP_CREATED_DATE,
		{{ ref('CgAdd_Flag1') }}.APP_CREATED_BY_STAFF_NUMBER,
		{{ ref('CgAdd_Flag1') }}.APP_OWNED_BY_STAFF_NUMBER,
		{{ ref('CgAdd_Flag1') }}.APP_CHANNEL_CAT_ID,
		{{ ref('CgAdd_Flag1') }}.APP_LODGEMENT_BRANCH_ID,
		{{ ref('CgAdd_Flag1') }}.APP_SM_CASE_ID,
		{{ ref('CgAdd_Flag2') }}.APP_ID AS CCL_APP_CCL_APP_ID,
		{{ ref('CgAdd_Flag2') }}.CCL_APP_CCL_APP_CAT_ID,
		{{ ref('CgAdd_Flag2') }}.CCL_APP_CCL_FORM_CAT_ID,
		{{ ref('CgAdd_Flag2') }}.CCL_APP_TOT_PERSONAL_FAC_AMT,
		{{ ref('CgAdd_Flag2') }}.CCL_APP_TOT_EQUIPFIN_FAC_AMT,
		{{ ref('CgAdd_Flag2') }}.CCL_APP_TOT_COMMERCIAL_FAC_AMT,
		{{ ref('CgAdd_Flag2') }}.CCL_APP_TOPUP_APP_ID,
		{{ ref('CgAdd_Flag2') }}.CCL_APP_AF_PRIMARY_INDUSTRY_ID,
		{{ ref('CgAdd_Flag2') }}.CCL_APP_AD_TUC_AMT,
		{{ ref('CgAdd_Flag2') }}.CCL_APP_COMMISSION_AMT,
		{{ ref('CgAdd_Flag2') }}.CCL_APP_BROKER_REFERAL_FLAG,
		{{ ref('CgAdd_Flag1') }}.APP_FOUND_FLAG,
		{{ ref('CgAdd_Flag2') }}.CCL_APP_FOUND_FLAG
	FROM {{ ref('CgAdd_Flag1') }}
	OUTER JOIN {{ ref('CgAdd_Flag2') }} ON {{ ref('CgAdd_Flag1') }}.APP_ID = {{ ref('CgAdd_Flag2') }}.APP_ID
)

SELECT * FROM JoinAppCclApp