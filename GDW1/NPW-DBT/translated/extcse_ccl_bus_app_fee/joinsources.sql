{{ config(materialized='view', tags=['ExtCSE_CCL_BUS_APP_FEE']) }}

WITH JoinSources AS (
	SELECT
		{{ ref('CpyRemoveCols2') }}.CCL_APP_FEE_ID,
		{{ ref('CpyRemoveCols2') }}.CCL_APP_FEE_CCL_APP_ID,
		{{ ref('CpyRemoveCols2') }}.CCL_APP_FEE_CCL_APP_PROD_ID,
		{{ ref('CpyRemoveCols2') }}.CCL_APP_FEE_CHARGE_AMT,
		{{ ref('CpyRemoveCols2') }}.CCL_APP_FEE_CHARGE_DATE,
		{{ ref('CpyRemoveCols2') }}.CCL_APP_FEE_CONCESSION_FLAG,
		{{ ref('CpyRemoveCols2') }}.CCL_APP_FEE_CONCESSION_REASON,
		{{ ref('CpyRemoveCols2') }}.CCL_APP_FEE_OVERRIDE_FEE_PCT,
		{{ ref('CpyRemoveCols2') }}.CCL_APP_FEE_OVERRIDE_FEE_PCT_FREQ,
		{{ ref('CpyRemoveCols2') }}.CCL_APP_FEE_FEE_REPAYMENT_FREQ,
		{{ ref('CpyRemoveCols2') }}.CCL_APP_FEE_CHARGE_EXTERNAL_FLAG,
		{{ ref('CpyRemoveCols2') }}.CCL_FEE_TYPE_CAT_ID,
		{{ ref('CpyRemoveCols') }}.CCL_FEE_TYPE_CAT_FEE_TYPE_DESC,
		{{ ref('CpyRemoveCols') }}.CCL_FEE_TYPE_CAT_DEFAULT_AMT,
		{{ ref('CpyRemoveCols') }}.CCL_FEE_TYPE_CAT_DEFAULT_FEE_PCT
	FROM {{ ref('CpyRemoveCols2') }}
	LEFT JOIN {{ ref('CpyRemoveCols') }} ON {{ ref('CpyRemoveCols2') }}.CCL_FEE_TYPE_CAT_ID = {{ ref('CpyRemoveCols') }}.CCL_FEE_TYPE_CAT_ID
)

SELECT * FROM JoinSources