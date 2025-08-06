{{ config(materialized='view', tags=['MergeBusHlmAppTpb']) }}

WITH JoinChlBusHlmAppCHlBusApp AS (
	SELECT
		{{ ref('CgAdd_Flag2') }}.APP_ID,
		{{ ref('CgAdd_Flag2') }}.HLM_APP_RECORD_TYPE,
		{{ ref('CgAdd_Flag2') }}.HLM_APP_MOD_TIMESTAMP,
		{{ ref('CgAdd_Flag2') }}.HLM_APP__ACCOUNT_ID,
		{{ ref('CgAdd_Flag2') }}.HLM_APP_ACCOUNT_NUMBER,
		{{ ref('CgAdd_Flag2') }}.HLM_APP_CRIS_PRODUCT_ID,
		{{ ref('CgAdd_Flag2') }}.HLM_APP_TYPE_CAT_ID,
		{{ ref('CgAdd_Flag2') }}.HLM_APP_DISCHARGE_REASON_ID,
		{{ ref('CgAdd_Flag2') }}.HLM_APP_PROD_ID,
		{{ ref('CgAdd_Flag2') }}.PEXA_FLAG,
		{{ ref('CgAdd_Flag2') }}.DISCHARGE_EXTERNAL_OFI_ID,
		{{ ref('CgAdd_Flag2') }}.INSTITUTION_NAME,
		{{ ref('CgAdd_Flag2') }}.HLM_APP_FOUND_FLAG,
		{{ ref('CgAdd_Flag1') }}.CHL_TPB_RECORD_TYPE,
		{{ ref('CgAdd_Flag1') }}.CHL_TPB_MOD_TIMESTAMP,
		{{ ref('CgAdd_Flag1') }}.CHL_TPB_SUBTYPE_CODE,
		{{ ref('CgAdd_Flag1') }}.REL_MANAGER_STATE_ID,
		{{ ref('CgAdd_Flag1') }}.MOD_USER_ID,
		{{ ref('CgAdd_Flag1') }}.DATE_RECEIVED,
		{{ ref('CgAdd_Flag1') }}.HL_BUSINESS_CHANNEL_CAT_ID,
		{{ ref('CgAdd_Flag1') }}.CHL_AGENT_ALIAS_ID,
		{{ ref('CgAdd_Flag1') }}.CHL_AGENT_NAME,
		{{ ref('CgAdd_Flag1') }}.CHL_TPB_FOUND_FLAG
	FROM {{ ref('CgAdd_Flag2') }}
	LEFT JOIN {{ ref('CgAdd_Flag1') }} ON {{ ref('CgAdd_Flag2') }}.APP_ID = {{ ref('CgAdd_Flag1') }}.APP_ID
)

SELECT * FROM JoinChlBusHlmAppCHlBusApp