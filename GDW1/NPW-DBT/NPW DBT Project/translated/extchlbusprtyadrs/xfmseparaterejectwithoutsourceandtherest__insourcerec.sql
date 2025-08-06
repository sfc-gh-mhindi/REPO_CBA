{{ config(materialized='view', tags=['ExtChlBusPrtyAdrs']) }}

WITH XfmSeparateRejectWithoutSourceAndTheRest__InSourceRec AS (
	SELECT
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((XfmSeparateRejects.CHL_APP_HL_APP_ID)) THEN (XfmSeparateRejects.CHL_APP_HL_APP_ID) ELSE ""))) = 0) Then 'R' Else 'S',
		IFF(LEN(TRIM(IFF({{ ref('JoinSrcSortReject') }}.CHL_APP_HL_APP_ID IS NOT NULL, {{ ref('JoinSrcSortReject') }}.CHL_APP_HL_APP_ID, ''))) = 0, 'R', 'S') AS DeltaFlag,
		CHL_APP_HL_APP_ID,
		CHL_APP_SUBTYPE_CODE,
		CHL_APP_SECURITY_ID,
		CHL_ASSET_LIABILITY_ID,
		CHL_PRINCIPAL_SECURITY_FLAG,
		CHL_ADDRESS_LINE_1,
		CHL_ADDRESS_LINE_2,
		CHL_SUBURB,
		CHL_STATE,
		CHL_POSTCODE,
		CHL_DPID,
		CHL_COUNTRY_ID,
		ETL_PROCESS_DT AS ORIG_ETL_D
	FROM {{ ref('JoinSrcSortReject') }}
	WHERE DeltaFlag = 'S'
)

SELECT * FROM XfmSeparateRejectWithoutSourceAndTheRest__InSourceRec