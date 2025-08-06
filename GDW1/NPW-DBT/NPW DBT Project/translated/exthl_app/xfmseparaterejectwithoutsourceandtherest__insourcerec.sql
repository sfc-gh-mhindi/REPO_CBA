{{ config(materialized='view', tags=['ExtHL_APP']) }}

WITH XfmSeparateRejectWithoutSourceAndTheRest__InSourceRec AS (
	SELECT
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((XfmSeparateRejects.HL_APP_ID)) THEN (XfmSeparateRejects.HL_APP_ID) ELSE "")))) = 0 Then 'R' Else 'S',
		IFF(LEN(TRIM(IFF({{ ref('JoinSrcSortReject') }}.HL_APP_ID IS NOT NULL, {{ ref('JoinSrcSortReject') }}.HL_APP_ID, ''))) = 0, 'R', 'S') AS DeltaFlag,
		HL_APP_ID,
		HL_APP_PROD_ID,
		HL_PACKAGE_CAT_ID,
		LPC_OFFICE,
		STATUS_TRACKER_ID,
		CHL_APP_PCD_EXTERNAL_SYS_CAT_ID,
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
		HSCA_CONVERTED_TO_FULL_AT,
		ETL_PROCESS_DT AS ORIG_ETL_D
	FROM {{ ref('JoinSrcSortReject') }}
	WHERE DeltaFlag = 'S'
)

SELECT * FROM XfmSeparateRejectWithoutSourceAndTheRest__InSourceRec