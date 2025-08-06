{{ config(materialized='view', tags=['MergeChlIntRateChlIntRatePercChlProdIntMarg']) }}

WITH Identify_HL_INT_RATE_ID_IntRateIntRatePerc AS (
	SELECT
		-- *SRC*: \(20)If IsNull(OutDropColumns.RATE_HL_INT_RATE_ID) then OutDropColumns.PERC_HL_INT_RATE_ID else OutDropColumns.RATE_HL_INT_RATE_ID,
		IFF({{ ref('JoinIntRateIntRatePerc') }}.RATE_HL_INT_RATE_ID IS NULL, {{ ref('JoinIntRateIntRatePerc') }}.PERC_HL_INT_RATE_ID, {{ ref('JoinIntRateIntRatePerc') }}.RATE_HL_INT_RATE_ID) AS HL_INT_RATE_ID,
		RATE_HL_INT_RATE_ID,
		RATE_HL_APP_PROD_ID,
		RATE_CASS_INT_RATE_TYPE_CODE,
		RATE_RATE_SEQUENCE,
		RATE_DURATION,
		PERC_HL_INT_RATE_ID,
		PERC_SCHEDULE_RATE_TYPE,
		PERC_RATE_PERCENT_1,
		PERC_RATE_PERCENT_2,
		RATE_FOUND_FLAG,
		PERC_FOUND_FLAG
	FROM {{ ref('JoinIntRateIntRatePerc') }}
	WHERE 
)

SELECT * FROM Identify_HL_INT_RATE_ID_IntRateIntRatePerc