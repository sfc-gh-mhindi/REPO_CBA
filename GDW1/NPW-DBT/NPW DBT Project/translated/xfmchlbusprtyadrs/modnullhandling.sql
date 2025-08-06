{{ config(materialized='view', tags=['XfmChlBusPrtyAdrs']) }}

WITH ModNullHandling AS (
	SELECT 
	--Manual
	--APPT_QLFY_C: nullable string[2]= handle_null (APPT_QLFY_C, '99')
	--ISO_CNTY_C: nullable string[2]= handle_null (ISO_CNTY_C, '99')
	CHL_APP_HL_APP_ID, SBTY_CODE, CHL_APP_SECURITY_ID, CHL_ASSET_LIABILITY_ID, CHL_PRINCIPAL_SECURITY_FLAG, CHL_ADDRESS_LINE_1, CHL_ADDRESS_LINE_2, CHL_SUBURB, CHL_STATE, CHL_POSTCODE, CHL_DPID, CHL_COUNTRY_ID, ORIG_ETL_D, APPT_QLFY_C, ISO_CNTY_C 
	FROM {{ ref('LkpReferences') }}
)

SELECT * FROM ModNullHandling