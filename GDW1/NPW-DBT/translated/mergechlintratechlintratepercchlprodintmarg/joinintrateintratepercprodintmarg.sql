{{ config(materialized='view', tags=['MergeChlIntRateChlIntRatePercChlProdIntMarg']) }}

WITH JoinintRateIntRatePercProdIntMarg AS (
	SELECT
		{{ ref('Identify_HL_INT_RATE_ID_IntRateIntRatePerc') }}.HL_INT_RATE_ID AS RATE_PERC_HL_INT_RATE_ID,
		{{ ref('Identify_HL_INT_RATE_ID_IntRateIntRatePerc') }}.RATE_HL_INT_RATE_ID,
		{{ ref('Identify_HL_INT_RATE_ID_IntRateIntRatePerc') }}.RATE_HL_APP_PROD_ID,
		{{ ref('Identify_HL_INT_RATE_ID_IntRateIntRatePerc') }}.RATE_CASS_INT_RATE_TYPE_CODE,
		{{ ref('Identify_HL_INT_RATE_ID_IntRateIntRatePerc') }}.RATE_RATE_SEQUENCE,
		{{ ref('Identify_HL_INT_RATE_ID_IntRateIntRatePerc') }}.RATE_DURATION,
		{{ ref('Identify_HL_INT_RATE_ID_IntRateIntRatePerc') }}.PERC_HL_INT_RATE_ID,
		{{ ref('Identify_HL_INT_RATE_ID_IntRateIntRatePerc') }}.PERC_SCHEDULE_RATE_TYPE,
		{{ ref('Identify_HL_INT_RATE_ID_IntRateIntRatePerc') }}.PERC_RATE_PERCENT_1,
		{{ ref('Identify_HL_INT_RATE_ID_IntRateIntRatePerc') }}.PERC_RATE_PERCENT_2,
		{{ ref('Identify_HL_INT_RATE_ID_IntRateIntRatePerc') }}.RATE_FOUND_FLAG,
		{{ ref('Identify_HL_INT_RATE_ID_IntRateIntRatePerc') }}.PERC_FOUND_FLAG,
		{{ ref('RD_KeepLatestMargin') }}.MARG_HL_PROD_INT_MARGIN_ID,
		{{ ref('RD_KeepLatestMargin') }}.HL_INT_RATE_ID AS MARG_HL_INT_RATE_ID,
		{{ ref('RD_KeepLatestMargin') }}.MARG_HL_PROD_INT_MARGIN_CAT_ID,
		{{ ref('RD_KeepLatestMargin') }}.MARG_MARGIN_TYPE,
		{{ ref('RD_KeepLatestMargin') }}.MARG_MARGIN_DESC,
		{{ ref('RD_KeepLatestMargin') }}.MARG_MARGIN_CODE,
		{{ ref('RD_KeepLatestMargin') }}.MARG_MARGIN_AMOUNT,
		{{ ref('RD_KeepLatestMargin') }}.MARG_ADJ_AMT,
		{{ ref('RD_KeepLatestMargin') }}.MARG_FOUND_FLAG
	FROM {{ ref('Identify_HL_INT_RATE_ID_IntRateIntRatePerc') }}
	OUTER JOIN {{ ref('RD_KeepLatestMargin') }} ON {{ ref('Identify_HL_INT_RATE_ID_IntRateIntRatePerc') }}.HL_INT_RATE_ID = {{ ref('RD_KeepLatestMargin') }}.HL_INT_RATE_ID
)

SELECT * FROM JoinintRateIntRatePercProdIntMarg