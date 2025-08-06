{{ config(materialized='view', tags=['MergeChlIntRateChlIntRatePercChlProdIntMarg']) }}

WITH JoinIntRateIntRatePerc AS (
	SELECT
		{{ ref('CgAdd_Flag1') }}.HL_INT_RATE_ID AS RATE_HL_INT_RATE_ID,
		{{ ref('CgAdd_Flag1') }}.RATE_HL_APP_PROD_ID,
		{{ ref('CgAdd_Flag1') }}.RATE_CASS_INT_RATE_TYPE_CODE,
		{{ ref('CgAdd_Flag1') }}.RATE_RATE_SEQUENCE,
		{{ ref('CgAdd_Flag1') }}.RATE_DURATION,
		{{ ref('Identify_HL_INT_RATE_ID_IntRatePerc') }}.HL_INT_RATE_ID AS PERC_HL_INT_RATE_ID,
		{{ ref('Identify_HL_INT_RATE_ID_IntRatePerc') }}.PERC_SCHEDULE_RATE_TYPE,
		{{ ref('Identify_HL_INT_RATE_ID_IntRatePerc') }}.PERC_RATE_PERCENT_1,
		{{ ref('Identify_HL_INT_RATE_ID_IntRatePerc') }}.PERC_RATE_PERCENT_2,
		{{ ref('CgAdd_Flag1') }}.RATE_FOUND_FLAG,
		{{ ref('Identify_HL_INT_RATE_ID_IntRatePerc') }}.PERC_FOUND_FLAG
	FROM {{ ref('CgAdd_Flag1') }}
	OUTER JOIN {{ ref('Identify_HL_INT_RATE_ID_IntRatePerc') }} ON {{ ref('CgAdd_Flag1') }}.HL_INT_RATE_ID = {{ ref('Identify_HL_INT_RATE_ID_IntRatePerc') }}.HL_INT_RATE_ID
)

SELECT * FROM JoinIntRateIntRatePerc