{{ config(materialized='view', tags=['XfmCSE_CCL_BUS_APP_FEEFrmExt']) }}

WITH LkpReferences AS (
	SELECT
		{{ ref('UpperCaseCol') }}.CCL_APP_FEE_ID,
		{{ ref('UpperCaseCol') }}.CCL_APP_FEE_CCL_APP_ID,
		{{ ref('UpperCaseCol') }}.CCL_APP_FEE_CCL_APP_PROD_ID,
		{{ ref('UpperCaseCol') }}.CCL_APP_FEE_CHARGE_AMT,
		{{ ref('UpperCaseCol') }}.CCL_APP_FEE_CHARGE_DATE,
		{{ ref('UpperCaseCol') }}.CCL_APP_FEE_CONCESSION_FLAG,
		{{ ref('UpperCaseCol') }}.CCL_APP_FEE_CONCESSION_REASON,
		{{ ref('UpperCaseCol') }}.CCL_APP_FEE_OVERRIDE_FEE_PCT,
		{{ ref('UpperCaseCol') }}.OVRD_FEE_PCT_FREQ AS CCL_APP_FEE_OVERRIDE_FEE_PCT_FREQ,
		{{ ref('UpperCaseCol') }}.CCL_APP_FEE_FEE_REPAYMENT_FREQ,
		{{ ref('UpperCaseCol') }}.CCL_APP_FEE_TYPE_CAT_ID,
		{{ ref('UpperCaseCol') }}.CCL_APP_FEE_CHARGE_EXTERNAL_FLAG,
		{{ ref('UpperCaseCol') }}.SRCE_NUMC_1_C AS CCL_FEE_TYPE_CAT_ID,
		{{ ref('UpperCaseCol') }}.CCL_FEE_TYPE_CAT_DEFAULT_AMT,
		{{ ref('UpperCaseCol') }}.CCL_FEE_TYPE_CAT_DEFAULT_FEE_PCT,
		{{ ref('UpperCaseCol') }}.CCL_FEE_TYPE_CAT_FEE_TYPE_DESC,
		{{ ref('UpperCaseCol') }}.ORIG_ETL_D,
		{{ ref('UpperCaseCol') }}.MAP_TYPE_C,
		{{ ref('SrcMAP_CSE_OVRD_FEE_FRQ_CL_OVERRIDE_FEE_PCT_FREQ') }}.FREQ_IN_MTHS,
		{{ ref('SrcGRD_GNRC_MAP_MAP_TYPE_C_SRCE_NUMC_1_C') }}.TARG_CHAR_C
	FROM {{ ref('UpperCaseCol') }}
	LEFT JOIN {{ ref('SrcMAP_CSE_OVRD_FEE_FRQ_CL_OVERRIDE_FEE_PCT_FREQ') }} ON 
	LEFT JOIN {{ ref('SrcGRD_GNRC_MAP_MAP_TYPE_C_SRCE_NUMC_1_C') }} ON 
)

SELECT * FROM LkpReferences