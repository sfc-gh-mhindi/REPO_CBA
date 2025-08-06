{{ config(materialized='view', tags=['XfmPL_APP_PRODFrmExt']) }}

WITH LkpReferences AS (
	SELECT
		{{ ref('AddGrdColLkpKey') }}.PL_APP_PROD_ID,
		{{ ref('AddGrdColLkpKey') }}.PL_TARGET_CAT_ID,
		{{ ref('AddGrdColLkpKey') }}.REPAY_APPROX_AMT,
		{{ ref('AddGrdColLkpKey') }}.REPAY_FREQUENCY_ID,
		{{ ref('AddGrdColLkpKey') }}.PL_APP_PROD_REPAY_CAT_ID,
		{{ ref('AddGrdColLkpKey') }}.PL_PROD_TERM_CAT_ID,
		{{ ref('AddGrdColLkpKey') }}.PL_CAMPAIGN_CAT_ID,
		{{ ref('AddGrdColLkpKey') }}.AD_HOC_CAMPAIGN_DESC,
		{{ ref('AddGrdColLkpKey') }}.CAR_SEEKER_FLAG,
		{{ ref('AddGrdColLkpKey') }}.PL_PROD_TARGET_CAT_ID,
		{{ ref('AddGrdColLkpKey') }}.PL_MARKETING_SOURCE_CAT_ID,
		{{ ref('AddGrdColLkpKey') }}.HLS_ACCT_NO,
		{{ ref('AddGrdColLkpKey') }}.TOTAL_INTEREST_AMT,
		{{ ref('AddGrdColLkpKey') }}.APP_PROD_AMT,
		{{ ref('AddGrdColLkpKey') }}.TP_BROKER_ID,
		{{ ref('AddGrdColLkpKey') }}.TP_BROKER_FIRST_NAME,
		{{ ref('AddGrdColLkpKey') }}.TP_BROKER_LAST_NAME,
		{{ ref('AddGrdColLkpKey') }}.TP_BROKER_GROUP_CAT_ID,
		{{ ref('AddGrdColLkpKey') }}.READ_COSTS_AND_RISKS_FLAG,
		{{ ref('AddGrdColLkpKey') }}.ACCEPTS_COSTS_AND_RISKS_DATE,
		{{ ref('AddGrdColLkpKey') }}.MAP_TYPE_C,
		{{ ref('AddGrdColLkpKey') }}.ORIG_ETL_D,
		{{ ref('SrcMAP_CSE_UNID_PATY_CATG_PL_Lks') }}.UNID_PATY_CATG_C,
		{{ ref('SrcGRD_GNRC_MAP_SRCE_CHAR_1_C_Lks') }}.TARG_CHAR_C,
		{{ ref('SrcMAP_CSE_LOAN_TERM_PL_Lks') }}.SRCE_SYST_ACTL_TERM_Q,
		{{ ref('SrcMAP_CSE_PAYT_FREQ_Lks') }}.PAYT_FREQ_C,
		{{ ref('AddGrdColLkpKey') }}.MAP_TYPE_C2,
		{{ ref('SrcGRD_GNRC_MAP_SRCE_CHAR_1_C7_Lks') }}.TARG_CHAR_C2
	FROM {{ ref('AddGrdColLkpKey') }}
	LEFT JOIN {{ ref('SrcGRD_GNRC_MAP_SRCE_CHAR_1_C_Lks') }} ON 
	LEFT JOIN {{ ref('SrcMAP_CSE_UNID_PATY_CATG_PL_Lks') }} ON 
	LEFT JOIN {{ ref('SrcMAP_CSE_LOAN_TERM_PL_Lks') }} ON 
	LEFT JOIN {{ ref('SrcMAP_CSE_PAYT_FREQ_Lks') }} ON 
	LEFT JOIN {{ ref('SrcGRD_GNRC_MAP_SRCE_CHAR_1_C7_Lks') }} ON 
)

SELECT * FROM LkpReferences