{{ config(materialized='view', tags=['ExtHL_APP']) }}

WITH XfmCheckHlAppIdNulls__OutCheckHlAppIdNullsSorted AS (
	SELECT
		-- *SRC*: \(20)If (Trim(( IF IsNotNull((XfmCheckHlAppIdNulls.HL_APP_ID)) THEN (XfmCheckHlAppIdNulls.HL_APP_ID) ELSE 0)) = 0) Then 'REJ2010' else  if Trim(XfmCheckHlAppIdNulls.HL_APP_ID) = '' Then 'REJ2010' else  if num(XfmCheckHlAppIdNulls.HL_APP_ID) then ( if (StringToDecimal(TRIM(XfmCheckHlAppIdNulls.HL_APP_ID)) = 0) Then 'REJ2010' else '') else '',
		IFF(TRIM(IFF({{ ref('CpyHlAppSeq') }}.HL_APP_ID IS NOT NULL, {{ ref('CpyHlAppSeq') }}.HL_APP_ID, 0)) = 0, 'REJ2010', IFF(TRIM({{ ref('CpyHlAppSeq') }}.HL_APP_ID) = '', 'REJ2010', IFF(NUM({{ ref('CpyHlAppSeq') }}.HL_APP_ID), IFF(STRINGTODECIMAL(TRIM({{ ref('CpyHlAppSeq') }}.HL_APP_ID)) = 0, 'REJ2010', ''), ''))) AS ErrorCode,
		HL_APP_ID,
		HL_APP_PROD_ID,
		HL_PACKAGE_CAT_ID,
		LPC_OFFICE,
		STATUS_TRACKER_ID,
		CHL_APP_PCD_EXT_SYS_CAT_ID,
		CHL_APP_SIMPLE_APP_FLAG,
		CHL_APP_ORIGINATING_AGENT_ID,
		CHL_APP_AGENT_NAME,
		CASS_WITHHOLD_RISKBANK_FLAG,
		CR_DATE,
		ASSESSMENT_DATE,
		NCPR_FLAG,
		CAMPAIGN_CODE,
		FHB_FLAG,
		SETTLEMENT_DATE,
		PEXA_FLAG,
		HSCA_FLAG,
		HSCA_CONVERTED_TO_FULL_AT
	FROM {{ ref('CpyHlAppSeq') }}
	WHERE SUBSTRING(ErrorCode, 1, 3) <> 'REJ'
)

SELECT * FROM XfmCheckHlAppIdNulls__OutCheckHlAppIdNullsSorted