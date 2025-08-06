{{ config(materialized='view', tags=['LdREJT_CSE_CHL_PRTY_ADRS']) }}

WITH ModConversions AS (
	SELECT 
	--Manual
	--ETL_D= date_from_string[%yyyy%mm%dd] (ETL_D)
	--ORIG_ETL_D= date_from_string[%yyyy%mm%dd] (ORIG_ETL_D)
	CHL_APP_HL_APP_ID, CHL_APP_SUBTYPE_CODE, CHL_APP_SECURITY_ID, CHL_ASSET_LIABILITY_ID, CHL_PRINCIPAL_SECURITY_FLAG AS CHL_PRCP_SCUY_FLAG, CHL_ADDRESS_LINE_1, CHL_ADDRESS_LINE_2, CHL_SUBURB, CHL_STATE, CHL_POSTCODE, CHL_DPID, CHL_COUNTRY_ID, ETL_D, ORIG_ETL_D, EROR_C 
	FROM {{ ref('TgtCsePrtyAdrsRejectsDS') }}
)

SELECT * FROM ModConversions