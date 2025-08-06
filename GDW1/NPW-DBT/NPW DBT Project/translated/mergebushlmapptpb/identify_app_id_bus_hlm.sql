{{ config(materialized='view', tags=['MergeBusHlmAppTpb']) }}

WITH Identify_App_ID_Bus_Hlm AS (
	SELECT
		APP_ID,
		HLM_APP_PROD_ID,
		-- *SRC*: ( IF IsNotNull((OutputDropcolumns.HLM_APP_FOUND_FLAG)) THEN (OutputDropcolumns.HLM_APP_FOUND_FLAG) ELSE ('N')),
		IFF({{ ref('JoinChlBusHlmAppCHlBusApp') }}.HLM_APP_FOUND_FLAG IS NOT NULL, {{ ref('JoinChlBusHlmAppCHlBusApp') }}.HLM_APP_FOUND_FLAG, 'N') AS HLM_APP_FOUND_FLAG,
		-- *SRC*: ( IF IsNotNull((OutputDropcolumns.CHL_TPB_FOUND_FLAG)) THEN (OutputDropcolumns.CHL_TPB_FOUND_FLAG) ELSE ('N')),
		IFF({{ ref('JoinChlBusHlmAppCHlBusApp') }}.CHL_TPB_FOUND_FLAG IS NOT NULL, {{ ref('JoinChlBusHlmAppCHlBusApp') }}.CHL_TPB_FOUND_FLAG, 'N') AS CHL_TPB_FOUND_FLAG,
		CHL_AGENT_ALIAS_ID,
		CHL_AGENT_NAME,
		HLM_APP__ACCOUNT_ID,
		HLM_APP_ACCOUNT_NUMBER,
		HLM_APP_CRIS_PRODUCT_ID,
		HLM_APP_TYPE_CAT_ID,
		HLM_APP_DISCHARGE_REASON_ID,
		PEXA_FLAG,
		{{ ref('JoinChlBusHlmAppCHlBusApp') }}.DISCHARGE_EXTERNAL_OFI_ID AS OFI_ID,
		{{ ref('JoinChlBusHlmAppCHlBusApp') }}.INSTITUTION_NAME AS OFI_NAME
	FROM {{ ref('JoinChlBusHlmAppCHlBusApp') }}
	WHERE 
)

SELECT * FROM Identify_App_ID_Bus_Hlm