{{ config(materialized='view', tags=['ExtChlBusPrtyAdrs']) }}

WITH XfmSeparateRejectWithoutSourceAndTheRest__InRejectWithoutSourceRec AS (
	SELECT
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((XfmSeparateRejects.CHL_APP_HL_APP_ID)) THEN (XfmSeparateRejects.CHL_APP_HL_APP_ID) ELSE ""))) = 0) Then 'R' Else 'S',
		IFF(LEN(TRIM(IFF({{ ref('JoinSrcSortReject') }}.CHL_APP_HL_APP_ID IS NOT NULL, {{ ref('JoinSrcSortReject') }}.CHL_APP_HL_APP_ID, ''))) = 0, 'R', 'S') AS DeltaFlag,
		CHL_APP_HL_APP_ID,
		{{ ref('JoinSrcSortReject') }}.CHL_APP_SUBTYPE_CODE_R AS CHL_APP_SUBTYPE_CODE,
		{{ ref('JoinSrcSortReject') }}.CHL_APP_SECURITY_ID_R AS CHL_APP_SECURITY_ID,
		CHL_ASSET_LIABILITY_ID,
		{{ ref('JoinSrcSortReject') }}.CHL_PRINCIPAL_SECURITY_FLAG_R AS CHL_PRINCIPAL_SECURITY_FLAG,
		{{ ref('JoinSrcSortReject') }}.CHL_ADDRESS_LINE_1_R AS CHL_ADDRESS_LINE_1,
		{{ ref('JoinSrcSortReject') }}.CHL_ADDRESS_LINE_2_R AS CHL_ADDRESS_LINE_2,
		{{ ref('JoinSrcSortReject') }}.CHL_SUBURB_R AS CHL_SUBURB,
		{{ ref('JoinSrcSortReject') }}.CHL_STATE_R AS CHL_STATE,
		{{ ref('JoinSrcSortReject') }}.CHL_POSTCODE_R AS CHL_POSTCODE,
		{{ ref('JoinSrcSortReject') }}.CHL_DPID_R AS CHL_DPID,
		{{ ref('JoinSrcSortReject') }}.CHL_COUNTRY_ID_R AS CHL_COUNTRY_ID,
		{{ ref('JoinSrcSortReject') }}.ORIG_ETL_D_R AS ORIG_ETL_D
	FROM {{ ref('JoinSrcSortReject') }}
	WHERE DeltaFlag = 'R'
)

SELECT * FROM XfmSeparateRejectWithoutSourceAndTheRest__InRejectWithoutSourceRec