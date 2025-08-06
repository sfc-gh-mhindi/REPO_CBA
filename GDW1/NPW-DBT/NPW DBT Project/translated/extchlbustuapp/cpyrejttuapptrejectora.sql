{{ config(materialized='view', tags=['ExtChlBusTuApp']) }}

WITH CpyRejtTuApptRejectOra AS (
	SELECT
		{{ ref('SrcRejtApptRelRejectOra') }}.SUBTYPE_CODE AS SUBTYPE_CODE_R,
		{{ ref('SrcRejtApptRelRejectOra') }}.HL_APP_PROD_ID AS HL_APP_PROD_ID_R,
		TU_APP_ID,
		{{ ref('SrcRejtApptRelRejectOra') }}.TU_ACCOUNT_ID AS TU_ACCOUNT_ID_R,
		{{ ref('SrcRejtApptRelRejectOra') }}.TU_DOCCOLLECT_METHOD_CAT_ID AS TU_DOCCOLLECT_METHOD_CAT_ID_R,
		{{ ref('SrcRejtApptRelRejectOra') }}.DOCCOLLECT_ADDRESS_TYPE_ID AS DOCCOLLECT_ADDRESS_TYPE_ID_R,
		{{ ref('SrcRejtApptRelRejectOra') }}.DOCCOLLECT_BSB AS DOCCOLLECT_BSB_R,
		{{ ref('SrcRejtApptRelRejectOra') }}.TU_DOCCOLLECT_ADDRESS_LINE_1 AS TU_DOCCOLLECT_ADDRESS_LINE_1_R,
		{{ ref('SrcRejtApptRelRejectOra') }}.TU_DOCCOLLECT_ADDRESS_LINE_2 AS TU_DOCCOLLECT_ADDRESS_LINE_2_R,
		{{ ref('SrcRejtApptRelRejectOra') }}.TU_DOCCOLLECT_SUBURB AS TU_DOCCOLLECT_SUBURB_R,
		{{ ref('SrcRejtApptRelRejectOra') }}.TU_DOCCOLLECT_STATE_ID AS TU_DOCCOLLECT_STATE_ID_R,
		{{ ref('SrcRejtApptRelRejectOra') }}.TU_DOCCOLLECT_POSTCODE AS TU_DOCCOLLECT_POSTCODE_R,
		{{ ref('SrcRejtApptRelRejectOra') }}.TU_DOCCOLLECT_COUNTRY_ID AS TU_DOCCOLLECT_COUNTRY_ID_R,
		{{ ref('SrcRejtApptRelRejectOra') }}.TU_DOCCOLLECT_OVERSEA_STATE AS TU_DOCCOLLECT_OVERSEA_STATE_R,
		{{ ref('SrcRejtApptRelRejectOra') }}.TOPUP_AMOUNT AS TOPUP_AMOUNT_R,
		{{ ref('SrcRejtApptRelRejectOra') }}.TOPUP_AGENT_ID AS TOPUP_AGENT_ID_R,
		{{ ref('SrcRejtApptRelRejectOra') }}.TOPUP_AGENT_NAME AS TOPUP_AGENT_NAME_R,
		{{ ref('SrcRejtApptRelRejectOra') }}.ACCOUNT_NO AS ACCOUNT_NO_R,
		{{ ref('SrcRejtApptRelRejectOra') }}.CRIS_PRODUCT_ID AS CRIS_PRODUCT_ID_R,
		{{ ref('SrcRejtApptRelRejectOra') }}.ORIG_ETL_D AS ORIG_ETL_D_R
	FROM {{ ref('SrcRejtApptRelRejectOra') }}
)

SELECT * FROM CpyRejtTuApptRejectOra