{{ config(materialized='view', tags=['XfmChlBusPrtyAdrs']) }}

WITH CpyRename AS (
	SELECT
		CHL_APP_HL_APP_ID,
		{{ ref('SrcAppCclAppPremapDS') }}.CHL_APP_SUBTYPE_CODE AS SBTY_CODE,
		CHL_APP_SECURITY_ID,
		CHL_ASSET_LIABILITY_ID,
		CHL_PRINCIPAL_SECURITY_FLAG,
		CHL_ADDRESS_LINE_1,
		CHL_ADDRESS_LINE_2,
		CHL_SUBURB,
		CHL_STATE,
		CHL_POSTCODE,
		CHL_DPID,
		{{ ref('SrcAppCclAppPremapDS') }}.CHL_COUNTRY_ID AS CNTY_ID,
		ORIG_ETL_D
	FROM {{ ref('SrcAppCclAppPremapDS') }}
)

SELECT * FROM CpyRename