{{ config(materialized='view', tags=['ExtChlBusPrtyAdrs']) }}

WITH CpyRejtRejectOra AS (
	SELECT
		CHL_APP_HL_APP_ID,
		{{ ref('SrcRejtRejectOra') }}.CHL_APP_SUBTYPE_CODE AS CHL_APP_SUBTYPE_CODE_R,
		{{ ref('SrcRejtRejectOra') }}.CHL_APP_SECURITY_ID AS CHL_APP_SECURITY_ID_R,
		CHL_ASSET_LIABILITY_ID,
		{{ ref('SrcRejtRejectOra') }}.CHL_PRCP_SCUY_FLAG AS CHL_PRINCIPAL_SECURITY_FLAG_R,
		{{ ref('SrcRejtRejectOra') }}.CHL_ADDRESS_LINE_1 AS CHL_ADDRESS_LINE_1_R,
		{{ ref('SrcRejtRejectOra') }}.CHL_ADDRESS_LINE_2 AS CHL_ADDRESS_LINE_2_R,
		{{ ref('SrcRejtRejectOra') }}.CHL_SUBURB AS CHL_SUBURB_R,
		{{ ref('SrcRejtRejectOra') }}.CHL_STATE AS CHL_STATE_R,
		{{ ref('SrcRejtRejectOra') }}.CHL_POSTCODE AS CHL_POSTCODE_R,
		{{ ref('SrcRejtRejectOra') }}.CHL_DPID AS CHL_DPID_R,
		{{ ref('SrcRejtRejectOra') }}.CHL_COUNTRY_ID AS CHL_COUNTRY_ID_R,
		{{ ref('SrcRejtRejectOra') }}.ORIG_ETL_D AS ORIG_ETL_D_R
	FROM {{ ref('SrcRejtRejectOra') }}
)

SELECT * FROM CpyRejtRejectOra