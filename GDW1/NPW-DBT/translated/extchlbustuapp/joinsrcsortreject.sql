{{ config(materialized='view', tags=['ExtChlBusTuApp']) }}

WITH JoinSrcSortReject AS (
	SELECT
		{{ ref('CpyInTuApptSeq') }}.SUBTYPE_CODE,
		{{ ref('CpyInTuApptSeq') }}.HL_APP_PROD_ID,
		{{ ref('CpyInTuApptSeq') }}.TU_APP_ID,
		{{ ref('CpyInTuApptSeq') }}.TU_ACCOUNT_ID,
		{{ ref('CpyInTuApptSeq') }}.TU_DOCCOLLECT_METHOD_CAT_ID,
		{{ ref('CpyInTuApptSeq') }}.DOCCOLLECT_ADDRESS_TYPE_ID,
		{{ ref('CpyInTuApptSeq') }}.DOCCOLLECT_BSB,
		{{ ref('CpyInTuApptSeq') }}.TU_DOCCOLLECT_ADDRESS_LINE_1,
		{{ ref('CpyInTuApptSeq') }}.TU_DOCCOLLECT_ADDRESS_LINE_2,
		{{ ref('CpyInTuApptSeq') }}.TU_DOCCOLLECT_SUBURB,
		{{ ref('CpyInTuApptSeq') }}.TU_DOCCOLLECT_STATE_ID,
		{{ ref('CpyInTuApptSeq') }}.TU_DOCCOLLECT_POSTCODE,
		{{ ref('CpyInTuApptSeq') }}.TU_DOCCOLLECT_COUNTRY_ID,
		{{ ref('CpyInTuApptSeq') }}.TU_DOCCOLLECT_OVERSEA_STATE,
		{{ ref('CpyInTuApptSeq') }}.TOPUP_AMOUNT,
		{{ ref('CpyInTuApptSeq') }}.TOPUP_AGENT_ID,
		{{ ref('CpyInTuApptSeq') }}.TOPUP_AGENT_NAME,
		{{ ref('CpyInTuApptSeq') }}.ACCOUNT_NO,
		{{ ref('CpyInTuApptSeq') }}.CRIS_PRODUCT_ID,
		{{ ref('CpyRejtTuApptRejectOra') }}.SUBTYPE_CODE_R,
		{{ ref('CpyRejtTuApptRejectOra') }}.HL_APP_PROD_ID_R,
		{{ ref('CpyRejtTuApptRejectOra') }}.TU_APP_ID AS TU_APP_ID_R,
		{{ ref('CpyRejtTuApptRejectOra') }}.TU_ACCOUNT_ID_R,
		{{ ref('CpyRejtTuApptRejectOra') }}.TU_DOCCOLLECT_METHOD_CAT_ID_R,
		{{ ref('CpyRejtTuApptRejectOra') }}.DOCCOLLECT_ADDRESS_TYPE_ID_R,
		{{ ref('CpyRejtTuApptRejectOra') }}.DOCCOLLECT_BSB_R,
		{{ ref('CpyRejtTuApptRejectOra') }}.TU_DOCCOLLECT_ADDRESS_LINE_1_R,
		{{ ref('CpyRejtTuApptRejectOra') }}.TU_DOCCOLLECT_ADDRESS_LINE_2_R,
		{{ ref('CpyRejtTuApptRejectOra') }}.TU_DOCCOLLECT_SUBURB_R,
		{{ ref('CpyRejtTuApptRejectOra') }}.TU_DOCCOLLECT_STATE_ID_R,
		{{ ref('CpyRejtTuApptRejectOra') }}.TU_DOCCOLLECT_POSTCODE_R,
		{{ ref('CpyRejtTuApptRejectOra') }}.TU_DOCCOLLECT_COUNTRY_ID_R,
		{{ ref('CpyRejtTuApptRejectOra') }}.TU_DOCCOLLECT_OVERSEA_STATE_R,
		{{ ref('CpyRejtTuApptRejectOra') }}.TOPUP_AMOUNT_R,
		{{ ref('CpyRejtTuApptRejectOra') }}.TOPUP_AGENT_ID_R,
		{{ ref('CpyRejtTuApptRejectOra') }}.TOPUP_AGENT_NAME_R,
		{{ ref('CpyRejtTuApptRejectOra') }}.ACCOUNT_NO_R,
		{{ ref('CpyRejtTuApptRejectOra') }}.CRIS_PRODUCT_ID_R,
		{{ ref('CpyRejtTuApptRejectOra') }}.ORIG_ETL_D_R,
		{{ ref('CpyInTuApptSeq') }}.CAMPAIGN_CODE
	FROM {{ ref('CpyRejtTuApptRejectOra') }}
	OUTER JOIN {{ ref('CpyInTuApptSeq') }} ON {{ ref('CpyRejtTuApptRejectOra') }}.TU_APP_ID = {{ ref('CpyInTuApptSeq') }}.TU_APP_ID
)

SELECT * FROM JoinSrcSortReject