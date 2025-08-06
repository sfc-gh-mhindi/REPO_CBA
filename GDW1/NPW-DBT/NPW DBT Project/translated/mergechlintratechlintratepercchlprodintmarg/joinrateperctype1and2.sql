{{ config(materialized='view', tags=['MergeChlIntRateChlIntRatePercChlProdIntMarg']) }}

WITH JoinRatePercType1And2 AS (
	SELECT
		PERC_HL_INT_RATE_PERCENT_ID_1,
		{{ ref('SwDetermineRatePerc') }}.HL_INT_RATE_ID AS HL_INT_RATE_ID_1,
		PERC_RATE_PERCENT_1,
		PERC_SCHEDULE_RATE_TYPE_1,
		PERC_FOUND_FLAG_1,
		PERC_HL_INT_RATE_PERCENT_ID_2,
		{{ ref('SwDetermineRatePerc') }}.HL_INT_RATE_ID AS HL_INT_RATE_ID_2,
		PERC_RATE_PERCENT_2,
		PERC_SCHEDULE_RATE_TYPE_2,
		PERC_FOUND_FLAG_2
	FROM {{ ref('SwDetermineRatePerc') }}
	OUTER JOIN {{ ref('SwDetermineRatePerc') }} ON {{ ref('SwDetermineRatePerc') }}.HL_INT_RATE_ID = {{ ref('SwDetermineRatePerc') }}.HL_INT_RATE_ID
)

SELECT * FROM JoinRatePercType1And2