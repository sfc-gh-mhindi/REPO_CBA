{{ config(materialized='view', tags=['MergeChlIntRateChlIntRatePercChlProdIntMarg']) }}

WITH Identify_HL_INT_RATE_ID AS (
	SELECT
		-- *SRC*: \(20)If IsNull(OutDropColumns.RATE_PERC_HL_INT_RATE_ID) then OutDropColumns.MARG_HL_INT_RATE_ID else OutDropColumns.RATE_PERC_HL_INT_RATE_ID,
		IFF({{ ref('JoinintRateIntRatePercProdIntMarg') }}.RATE_PERC_HL_INT_RATE_ID IS NULL, {{ ref('JoinintRateIntRatePercProdIntMarg') }}.MARG_HL_INT_RATE_ID, {{ ref('JoinintRateIntRatePercProdIntMarg') }}.RATE_PERC_HL_INT_RATE_ID) AS HL_INT_RATE_ID,
		RATE_HL_INT_RATE_ID,
		RATE_HL_APP_PROD_ID,
		RATE_CASS_INT_RATE_TYPE_CODE,
		RATE_RATE_SEQUENCE,
		RATE_DURATION,
		PERC_HL_INT_RATE_ID,
		PERC_SCHEDULE_RATE_TYPE,
		PERC_RATE_PERCENT_1,
		PERC_RATE_PERCENT_2,
		MARG_HL_PROD_INT_MARGIN_ID,
		MARG_HL_INT_RATE_ID,
		MARG_HL_PROD_INT_MARGIN_CAT_ID,
		MARG_MARGIN_TYPE,
		MARG_MARGIN_DESC,
		MARG_MARGIN_CODE,
		MARG_MARGIN_AMOUNT,
		MARG_ADJ_AMT,
		-- *SRC*: ( IF IsNotNull((OutDropColumns.RATE_FOUND_FLAG)) THEN (OutDropColumns.RATE_FOUND_FLAG) ELSE ('N')),
		IFF({{ ref('JoinintRateIntRatePercProdIntMarg') }}.RATE_FOUND_FLAG IS NOT NULL, {{ ref('JoinintRateIntRatePercProdIntMarg') }}.RATE_FOUND_FLAG, 'N') AS RATE_FOUND_FLAG,
		-- *SRC*: ( IF IsNotNull((OutDropColumns.PERC_FOUND_FLAG)) THEN (OutDropColumns.PERC_FOUND_FLAG) ELSE ('N')),
		IFF({{ ref('JoinintRateIntRatePercProdIntMarg') }}.PERC_FOUND_FLAG IS NOT NULL, {{ ref('JoinintRateIntRatePercProdIntMarg') }}.PERC_FOUND_FLAG, 'N') AS PERC_FOUND_FLAG,
		-- *SRC*: ( IF IsNotNull((OutDropColumns.MARG_FOUND_FLAG)) THEN (OutDropColumns.MARG_FOUND_FLAG) ELSE ('N')),
		IFF({{ ref('JoinintRateIntRatePercProdIntMarg') }}.MARG_FOUND_FLAG IS NOT NULL, {{ ref('JoinintRateIntRatePercProdIntMarg') }}.MARG_FOUND_FLAG, 'N') AS MARG_FOUND_FLAG
	FROM {{ ref('JoinintRateIntRatePercProdIntMarg') }}
	WHERE 
)

SELECT * FROM Identify_HL_INT_RATE_ID