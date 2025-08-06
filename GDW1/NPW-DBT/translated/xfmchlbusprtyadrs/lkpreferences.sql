{{ config(materialized='view', tags=['XfmChlBusPrtyAdrs']) }}

WITH LkpReferences AS (
	SELECT
		{{ ref('CpyRename') }}.CHL_APP_HL_APP_ID,
		{{ ref('CpyRename') }}.SBTY_CODE,
		{{ ref('CpyRename') }}.CHL_APP_SECURITY_ID,
		{{ ref('CpyRename') }}.CHL_ASSET_LIABILITY_ID,
		{{ ref('CpyRename') }}.CHL_PRINCIPAL_SECURITY_FLAG,
		{{ ref('CpyRename') }}.CHL_ADDRESS_LINE_1,
		{{ ref('CpyRename') }}.CHL_ADDRESS_LINE_2,
		{{ ref('CpyRename') }}.CHL_SUBURB,
		{{ ref('CpyRename') }}.CHL_STATE,
		{{ ref('CpyRename') }}.CHL_POSTCODE,
		{{ ref('CpyRename') }}.CHL_DPID,
		{{ ref('CpyRename') }}.CNTY_ID AS CHL_COUNTRY_ID,
		{{ ref('CpyRename') }}.ORIG_ETL_D,
		{{ ref('SrcMAP_CSE_APPT_QLFYLks') }}.APPT_QLFY_C,
		{{ ref('TgtMAP_CSE_CNTY_CODELks') }}.ISO_CNTY_C
	FROM {{ ref('CpyRename') }}
	LEFT JOIN {{ ref('SrcMAP_CSE_APPT_QLFYLks') }} ON 
	LEFT JOIN {{ ref('TgtMAP_CSE_CNTY_CODELks') }} ON 
)

SELECT * FROM LkpReferences