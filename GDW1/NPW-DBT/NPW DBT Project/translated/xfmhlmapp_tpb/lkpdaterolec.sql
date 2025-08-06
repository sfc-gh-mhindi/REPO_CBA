{{ config(materialized='view', tags=['XfmHlmapp_Tpb']) }}

WITH LkpDateRoleC AS (
	SELECT
		{{ ref('XfmTrans__OutApptPdctPurp') }}.APP_ID,
		{{ ref('XfmTrans__OutApptPdctPurp') }}.HLM_APP_PROD_ID,
		{{ ref('XfmTrans__OutApptPdctPurp') }}.HLM_ACCOUNT_ID,
		{{ ref('XfmTrans__OutApptPdctPurp') }}.ACCOUNT_NUMBER,
		{{ ref('XfmTrans__OutApptPdctPurp') }}.CRIS_PRODUCT_ID,
		{{ ref('XfmTrans__OutApptPdctPurp') }}.HLM_APP_TYPE_CAT_ID,
		{{ ref('XfmTrans__OutApptPdctPurp') }}.DCHG_REAS_ID,
		{{ ref('XfmTrans__OutApptPdctPurp') }}.HL_APP_PROD_ID,
		{{ ref('XfmTrans__OutApptPdctPurp') }}.RUN_STRM,
		{{ ref('MAP_CSE_APPT_PDCT_PURP_HM') }}.PURP_TYPE_C
	FROM {{ ref('XfmTrans__OutApptPdctPurp') }}
	LEFT JOIN {{ ref('MAP_CSE_APPT_PDCT_PURP_HM') }} ON {{ ref('XfmTrans__OutApptPdctPurp') }}.DCHG_REAS_ID = {{ ref('MAP_CSE_APPT_PDCT_PURP_HM') }}.DCHG_REAS_I
)

SELECT * FROM LkpDateRoleC