{{ config(materialized='view', tags=['ExtHL_APP']) }}

WITH XfmSeparateRejectWithoutSourceAndTheRest__InRejectWithoutSourceRec AS (
	SELECT
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((XfmSeparateRejects.HL_APP_ID)) THEN (XfmSeparateRejects.HL_APP_ID) ELSE "")))) = 0 Then 'R' Else 'S',
		IFF(LEN(TRIM(IFF({{ ref('JoinSrcSortReject') }}.HL_APP_ID IS NOT NULL, {{ ref('JoinSrcSortReject') }}.HL_APP_ID, ''))) = 0, 'R', 'S') AS DeltaFlag,
		{{ ref('JoinSrcSortReject') }}.HL_APP_ID_R AS HL_APP_ID,
		{{ ref('JoinSrcSortReject') }}.HL_APP_PROD_ID_R AS HL_APP_PROD_ID,
		{{ ref('JoinSrcSortReject') }}.HL_PACKAGE_CAT_ID_R AS HL_PACKAGE_CAT_ID,
		{{ ref('JoinSrcSortReject') }}.LPC_OFFICE_R AS LPC_OFFICE,
		{{ ref('JoinSrcSortReject') }}.STATUS_TRACKER_ID_R AS STATUS_TRACKER_ID,
		{{ ref('JoinSrcSortReject') }}.CHL_APP_PCD_EXTERNAL_SYS_CAT_ID_R AS CHL_APP_PCD_EXTERNAL_SYS_CAT_ID,
		{{ ref('JoinSrcSortReject') }}.CHL_APP_SIMPLE_APP_FLAG_R AS CHL_APP_SIMPLE_APP_FLAG,
		{{ ref('JoinSrcSortReject') }}.CHL_APP_ORIGINATING_AGENT_ID_R AS CHL_APP_ORIGINATING_AGENT_ID,
		{{ ref('JoinSrcSortReject') }}.CHL_APP_AGENT_NAME_R AS CHL_APP_AGENT_NAME,
		{{ ref('JoinSrcSortReject') }}.CASS_WITHHOLD_RISKBANK_FLAG_R AS CASS_WITHHOLD_RISKBANK_FLAG,
		{{ ref('JoinSrcSortReject') }}.CR_DATE_R AS CR_DATE,
		{{ ref('JoinSrcSortReject') }}.ASSESSMENT_DATE_R AS ASSESSMENT_DATE,
		{{ ref('JoinSrcSortReject') }}.NCPR_FLAG_R AS NCPR_FLAG,
		-- *SRC*: SetNull(),
		SETNULL() AS CAMPAIGN_CODE,
		{{ ref('JoinSrcSortReject') }}.FHB_FLAG_R AS FHB_FLAG,
		{{ ref('JoinSrcSortReject') }}.SETTLEMENT_DATE_R AS SETTLEMENT_DATE,
		{{ ref('JoinSrcSortReject') }}.PEXA_FLAG_R AS PEXA_FLAG,
		{{ ref('JoinSrcSortReject') }}.HSCA_FLAG_R AS HSCA_FLAG,
		{{ ref('JoinSrcSortReject') }}.HSCA_CONVERTED_TO_FULL_AT_R AS HSCA_CONVERTED_TO_FULL_AT,
		{{ ref('JoinSrcSortReject') }}.ORIG_ETL_D_R AS ORIG_ETL_D
	FROM {{ ref('JoinSrcSortReject') }}
	WHERE DeltaFlag = 'R'
)

SELECT * FROM XfmSeparateRejectWithoutSourceAndTheRest__InRejectWithoutSourceRec