{{ config(materialized='view', tags=['MergeChlIntRateChlIntRatePercChlProdIntMarg']) }}

WITH Identify_HL_INT_RATE_ID_IntRatePerc AS (
	SELECT
		-- *SRC*: \(20)If Len(Trim(( IF IsNotNull((OutRatePerc.HL_INT_RATE_ID_1)) THEN (OutRatePerc.HL_INT_RATE_ID_1) ELSE ""))) = 0 Then OutRatePerc.HL_INT_RATE_ID_2 Else OutRatePerc.HL_INT_RATE_ID_1,
		IFF(LEN(TRIM(IFF({{ ref('JoinRatePercType1And2') }}.HL_INT_RATE_ID_1 IS NOT NULL, {{ ref('JoinRatePercType1And2') }}.HL_INT_RATE_ID_1, ''))) = 0, {{ ref('JoinRatePercType1And2') }}.HL_INT_RATE_ID_2, {{ ref('JoinRatePercType1And2') }}.HL_INT_RATE_ID_1) AS HL_INT_RATE_ID,
		{{ ref('JoinRatePercType1And2') }}.PERC_SCHEDULE_RATE_TYPE_1 AS PERC_SCHEDULE_RATE_TYPE,
		PERC_RATE_PERCENT_1,
		PERC_RATE_PERCENT_2,
		'Y' AS PERC_FOUND_FLAG
	FROM {{ ref('JoinRatePercType1And2') }}
	WHERE 
)

SELECT * FROM Identify_HL_INT_RATE_ID_IntRatePerc