{{ config(materialized='view', tags=['XfmHL_APP_PRODFrmExt']) }}

WITH LkpReferences AS (
	SELECT
		{{ ref('AddGrdColLkpKey') }}.HL_APP_PROD_ID,
		{{ ref('AddGrdColLkpKey') }}.PARENT_HL_APP_PROD_ID,
		{{ ref('AddGrdColLkpKey') }}.HL_REPAYMENT_PERIOD_CAT_ID,
		{{ ref('AddGrdColLkpKey') }}.AMOUNT,
		{{ ref('AddGrdColLkpKey') }}.LOAN_TERM_MONTHS,
		{{ ref('AddGrdColLkpKey') }}.ACCOUNT_NUMBER,
		{{ ref('AddGrdColLkpKey') }}.TOTAL_LOAN_AMOUNT,
		{{ ref('AddGrdColLkpKey') }}.HLS_FLAG,
		{{ ref('AddGrdColLkpKey') }}.GDW_UPDATED_LDP_PAID_ON_AMOUNT,
		{{ ref('AddGrdColLkpKey') }}.SRCE_CHAR_1_C,
		{{ ref('AddGrdColLkpKey') }}.ORIG_ETL_D,
		{{ ref('SrcMAP_CSE_PAYT_FREQLks') }}.PAYT_FREQ_C,
		{{ ref('SrcGRD_GNRC_MAP_SRCE_CHAR_1_CLks') }}.TARG_CHAR_C
	FROM {{ ref('AddGrdColLkpKey') }}
	LEFT JOIN {{ ref('SrcMAP_CSE_PAYT_FREQLks') }} ON 
	LEFT JOIN {{ ref('SrcGRD_GNRC_MAP_SRCE_CHAR_1_CLks') }} ON 
)

SELECT * FROM LkpReferences