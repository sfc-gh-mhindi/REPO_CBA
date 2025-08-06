{{ config(materialized='view', tags=['ExtChlBusPrtyAdrs']) }}

WITH JoinSrcSortReject AS (
	SELECT
		{{ ref('XfmCheckNulls') }}.CHL_APP_HL_APP_ID,
		{{ ref('XfmCheckNulls') }}.CHL_APP_SUBTYPE_CODE,
		{{ ref('XfmCheckNulls') }}.CHL_APP_SECURITY_ID,
		{{ ref('XfmCheckNulls') }}.CHL_ASSET_LIABILITY_ID,
		{{ ref('XfmCheckNulls') }}.CHL_PRINCIPAL_SECURITY_FLAG,
		{{ ref('XfmCheckNulls') }}.CHL_ADDRESS_LINE_1,
		{{ ref('XfmCheckNulls') }}.CHL_ADDRESS_LINE_2,
		{{ ref('XfmCheckNulls') }}.CHL_SUBURB,
		{{ ref('XfmCheckNulls') }}.CHL_STATE,
		{{ ref('XfmCheckNulls') }}.CHL_POSTCODE,
		{{ ref('XfmCheckNulls') }}.CHL_DPID,
		{{ ref('XfmCheckNulls') }}.CHL_COUNTRY_ID,
		{{ ref('CpyRejtRejectOra') }}.CHL_APP_SUBTYPE_CODE_R,
		{{ ref('CpyRejtRejectOra') }}.CHL_APP_SECURITY_ID_R,
		{{ ref('CpyRejtRejectOra') }}.CHL_PRINCIPAL_SECURITY_FLAG_R,
		{{ ref('CpyRejtRejectOra') }}.CHL_ADDRESS_LINE_1_R,
		{{ ref('CpyRejtRejectOra') }}.CHL_ADDRESS_LINE_2_R,
		{{ ref('CpyRejtRejectOra') }}.CHL_SUBURB_R,
		{{ ref('CpyRejtRejectOra') }}.CHL_STATE_R,
		{{ ref('CpyRejtRejectOra') }}.CHL_POSTCODE_R,
		{{ ref('CpyRejtRejectOra') }}.CHL_DPID_R,
		{{ ref('CpyRejtRejectOra') }}.CHL_COUNTRY_ID_R,
		{{ ref('CpyRejtRejectOra') }}.ORIG_ETL_D_R
	FROM {{ ref('XfmCheckNulls') }}
	OUTER JOIN {{ ref('CpyRejtRejectOra') }} ON {{ ref('XfmCheckNulls') }}.CHL_ASSET_LIABILITY_ID = {{ ref('CpyRejtRejectOra') }}.CHL_ASSET_LIABILITY_ID
	AND {{ ref('XfmCheckNulls') }}.CHL_APP_HL_APP_ID = {{ ref('CpyRejtRejectOra') }}.CHL_APP_HL_APP_ID
)

SELECT * FROM JoinSrcSortReject