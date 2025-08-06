{{ config(materialized='view', tags=['MergePlIntRatePlMargin']) }}

WITH Identify_PL_INT_RATE_AMT_PL_INT_RATE_ID AS (
	SELECT
		PL_INT_RATE_AMT_INT_RATE_AMT_2,
		PL_INT_RATE_AMT_PL_INT_RATE_CAT_ID_2,
		-- *SRC*: \(20)If Len(Trim(( IF IsNotNull((OutJoinCatID.leftRec_PL_INT_RATE_AMT_PL_INT_RATE_ID)) THEN (OutJoinCatID.leftRec_PL_INT_RATE_AMT_PL_INT_RATE_ID) ELSE ""))) = 0 Then OutJoinCatID.rightRec_PL_INT_RATE_AMT_PL_INT_RATE_ID Else OutJoinCatID.leftRec_PL_INT_RATE_AMT_PL_INT_RATE_ID,
		IFF(LEN(TRIM(IFF({{ ref('JoinCatID') }}.leftRec_PL_INT_RATE_AMT_PL_INT_RATE_ID IS NOT NULL, {{ ref('JoinCatID') }}.leftRec_PL_INT_RATE_AMT_PL_INT_RATE_ID, ''))) = 0, {{ ref('JoinCatID') }}.rightRec_PL_INT_RATE_AMT_PL_INT_RATE_ID, {{ ref('JoinCatID') }}.leftRec_PL_INT_RATE_AMT_PL_INT_RATE_ID) AS leftRec_PL_INT_RATE_AMT_PL_INT_RATE_ID,
		PL_INT_RATE_AMT_INT_RATE_AMT_1,
		PL_INT_RATE_AMT_PL_INT_RATE_CAT_ID_1
	FROM {{ ref('JoinCatID') }}
	WHERE 
)

SELECT * FROM Identify_PL_INT_RATE_AMT_PL_INT_RATE_ID