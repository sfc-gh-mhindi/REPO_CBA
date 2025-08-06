{{ config(materialized='view', tags=['ExtChlBusTuApp']) }}

WITH XfmSeparateRejectWithoutSourceAndTheRest__InRejectWithoutSourceRec AS (
	SELECT
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((XfmSeparateRejects.TU_APP_ID)) THEN (XfmSeparateRejects.TU_APP_ID) ELSE ""))) = 0) Then 'R' Else 'S',
		IFF(LEN(TRIM(IFF({{ ref('JoinSrcSortReject') }}.TU_APP_ID IS NOT NULL, {{ ref('JoinSrcSortReject') }}.TU_APP_ID, ''))) = 0, 'R', 'S') AS DeltaFlag,
		{{ ref('JoinSrcSortReject') }}.SUBTYPE_CODE_R AS SUBTYPE_CODE,
		{{ ref('JoinSrcSortReject') }}.HL_APP_PROD_ID_R AS HL_APP_PROD_ID,
		{{ ref('JoinSrcSortReject') }}.TU_APP_ID_R AS TU_APP_ID,
		{{ ref('JoinSrcSortReject') }}.TU_ACCOUNT_ID_R AS TU_ACCOUNT_ID,
		{{ ref('JoinSrcSortReject') }}.TU_DOCCOLLECT_METHOD_CAT_ID_R AS TU_DOCCOLLECT_METHOD_CAT_ID,
		{{ ref('JoinSrcSortReject') }}.DOCCOLLECT_ADDRESS_TYPE_ID_R AS DOCCOLLECT_ADDRESS_TYPE_ID,
		{{ ref('JoinSrcSortReject') }}.DOCCOLLECT_BSB_R AS DOCCOLLECT_BSB,
		{{ ref('JoinSrcSortReject') }}.TU_DOCCOLLECT_ADDRESS_LINE_1_R AS TU_DOCCOLLECT_ADDRESS_LINE_1,
		{{ ref('JoinSrcSortReject') }}.TU_DOCCOLLECT_ADDRESS_LINE_2_R AS TU_DOCCOLLECT_ADDRESS_LINE_2,
		{{ ref('JoinSrcSortReject') }}.TU_DOCCOLLECT_SUBURB_R AS TU_DOCCOLLECT_SUBURB,
		{{ ref('JoinSrcSortReject') }}.TU_DOCCOLLECT_STATE_ID_R AS TU_DOCCOLLECT_STATE_ID,
		{{ ref('JoinSrcSortReject') }}.TU_DOCCOLLECT_POSTCODE_R AS TU_DOCCOLLECT_POSTCODE,
		{{ ref('JoinSrcSortReject') }}.TU_DOCCOLLECT_COUNTRY_ID_R AS TU_DOCCOLLECT_COUNTRY_ID,
		{{ ref('JoinSrcSortReject') }}.TU_DOCCOLLECT_OVERSEA_STATE_R AS TU_DOCCOLLECT_OVERSEA_STATE,
		{{ ref('JoinSrcSortReject') }}.TOPUP_AMOUNT_R AS TOPUP_AMOUNT,
		{{ ref('JoinSrcSortReject') }}.TOPUP_AGENT_ID_R AS TOPUP_AGENT_ID,
		{{ ref('JoinSrcSortReject') }}.TOPUP_AGENT_NAME_R AS TOPUP_AGENT_NAME,
		{{ ref('JoinSrcSortReject') }}.ACCOUNT_NO_R AS ACCOUNT_NO,
		{{ ref('JoinSrcSortReject') }}.CRIS_PRODUCT_ID_R AS CRIS_PRODUCT_ID,
		-- *SRC*: SetNull(),
		SETNULL() AS CAMPAIGN_CODE,
		{{ ref('JoinSrcSortReject') }}.ORIG_ETL_D_R AS ORIG_ETL_D
	FROM {{ ref('JoinSrcSortReject') }}
	WHERE DeltaFlag = 'R'
)

SELECT * FROM XfmSeparateRejectWithoutSourceAndTheRest__InRejectWithoutSourceRec