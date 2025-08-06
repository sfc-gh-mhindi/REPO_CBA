{{ config(materialized='view', tags=['ExtChlBusTuApp']) }}

WITH XfmSeparateRejectWithoutSourceAndTheRest__InSourceRec AS (
	SELECT
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((XfmSeparateRejects.TU_APP_ID)) THEN (XfmSeparateRejects.TU_APP_ID) ELSE ""))) = 0) Then 'R' Else 'S',
		IFF(LEN(TRIM(IFF({{ ref('JoinSrcSortReject') }}.TU_APP_ID IS NOT NULL, {{ ref('JoinSrcSortReject') }}.TU_APP_ID, ''))) = 0, 'R', 'S') AS DeltaFlag,
		SUBTYPE_CODE,
		HL_APP_PROD_ID,
		TU_APP_ID,
		TU_ACCOUNT_ID,
		TU_DOCCOLLECT_METHOD_CAT_ID,
		DOCCOLLECT_ADDRESS_TYPE_ID,
		DOCCOLLECT_BSB,
		TU_DOCCOLLECT_ADDRESS_LINE_1,
		TU_DOCCOLLECT_ADDRESS_LINE_2,
		TU_DOCCOLLECT_SUBURB,
		TU_DOCCOLLECT_STATE_ID,
		TU_DOCCOLLECT_POSTCODE,
		TU_DOCCOLLECT_COUNTRY_ID,
		TU_DOCCOLLECT_OVERSEA_STATE,
		TOPUP_AMOUNT,
		TOPUP_AGENT_ID,
		TOPUP_AGENT_NAME,
		ACCOUNT_NO,
		CRIS_PRODUCT_ID,
		CAMPAIGN_CODE,
		ETL_PROCESS_DT AS ORIG_ETL_D
	FROM {{ ref('JoinSrcSortReject') }}
	WHERE DeltaFlag = 'S'
)

SELECT * FROM XfmSeparateRejectWithoutSourceAndTheRest__InSourceRec