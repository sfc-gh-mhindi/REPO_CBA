{{ config(materialized='view', tags=['XfmChlBusTuAPPFrmExt']) }}

WITH LkpReferences AS (
	SELECT
		{{ ref('CpyRename') }}.SBTY_CODE,
		{{ ref('CpyRename') }}.HL_APP_PROD_ID,
		{{ ref('CpyRename') }}.TU_APP_ID,
		{{ ref('CpyRename') }}.TU_ACCOUNT_ID,
		{{ ref('CpyRename') }}.TU_DOCCOLLECT_METHOD_CAT_ID,
		{{ ref('CpyRename') }}.ADRS_TYPE_ID,
		{{ ref('CpyRename') }}.DOCCOLLECT_BSB,
		{{ ref('CpyRename') }}.TU_DOCCOLLECT_ADDRESS_LINE_1,
		{{ ref('CpyRename') }}.TU_DOCCOLLECT_ADDRESS_LINE_2,
		{{ ref('CpyRename') }}.TU_DOCCOLLECT_SUBURB,
		{{ ref('CpyRename') }}.STAT_C,
		{{ ref('CpyRename') }}.TU_DOCCOLLECT_POSTCODE,
		{{ ref('CpyRename') }}.CNTY_ID,
		{{ ref('CpyRename') }}.TU_DOCCOLLECT_OVERSEA_STATE,
		{{ ref('CpyRename') }}.TOPUP_AMOUNT,
		{{ ref('CpyRename') }}.TOPUP_AGENT_ID,
		{{ ref('CpyRename') }}.TOPUP_AGENT_NAME,
		{{ ref('CpyRename') }}.ACCOUNT_NO,
		{{ ref('CpyRename') }}.CRIS_PDCT_C AS CRIS_PRODUCT_ID,
		{{ ref('CpyRename') }}.ORIG_ETL_D,
		{{ ref('SrcMAP_CSE_APPT_QLFYLks') }}.APPT_QLFY_C,
		{{ ref('TgtMAP_CSE_DOCU_DELY_METHLks') }}.DOCU_DELY_METH_C,
		{{ ref('TgtMAP_CSE_ADRS_TYPELks') }}.PYAD_TYPE_C,
		{{ ref('TgtMAP_CSE_STATELks') }}.STAT_X,
		{{ ref('TgtMAP_CSE_CNTY_CODELks') }}.ISO_CNTY_C,
		{{ ref('TgtMAP_CSE_CRIS_PDCTLks') }}.ACCT_QLFY_C,
		{{ ref('TgtMAP_CSE_CRIS_PDCTLks') }}.SRCE_SYST_C,
		{{ ref('CpyRename') }}.CAMPAIGN_CODE,
		OutTuApptPremapDS.SUBTYPE_CODE AS SBTY_CODE,
		OutTuApptPremapDS.HL_APP_PROD_ID,
		OutTuApptPremapDS.TU_APP_ID,
		OutTuApptPremapDS.TU_ACCOUNT_ID,
		OutTuApptPremapDS.TU_DOCCOLLECT_METHOD_CAT_ID,
		OutTuApptPremapDS.DOCCOLLECT_ADDRESS_TYPE_ID AS ADRS_TYPE_ID,
		OutTuApptPremapDS.DOCCOLLECT_BSB,
		OutTuApptPremapDS.TU_DOCCOLLECT_ADDRESS_LINE_1,
		OutTuApptPremapDS.TU_DOCCOLLECT_ADDRESS_LINE_2,
		OutTuApptPremapDS.TU_DOCCOLLECT_SUBURB,
		OutTuApptPremapDS.TU_DOCCOLLECT_STATE_ID AS STAT_C,
		OutTuApptPremapDS.TU_DOCCOLLECT_POSTCODE,
		OutTuApptPremapDS.TU_DOCCOLLECT_COUNTRY_ID AS CNTY_ID,
		OutTuApptPremapDS.TU_DOCCOLLECT_OVERSEA_STATE,
		OutTuApptPremapDS.TOPUP_AMOUNT,
		OutTuApptPremapDS.TOPUP_AGENT_ID,
		OutTuApptPremapDS.TOPUP_AGENT_NAME,
		OutTuApptPremapDS.ACCOUNT_NO,
		OutTuApptPremapDS.CRIS_PRODUCT_ID AS CRIS_PDCT_C,
		OutTuApptPremapDS.ORIG_ETL_D
	FROM {{ ref('CpyRename') }}
	LEFT JOIN {{ ref('SrcMAP_CSE_APPT_QLFYLks') }} ON 
	LEFT JOIN {{ ref('TgtMAP_CSE_ADRS_TYPELks') }} ON 
	LEFT JOIN {{ ref('TgtMAP_CSE_STATELks') }} ON 
	LEFT JOIN {{ ref('TgtMAP_CSE_CNTY_CODELks') }} ON 
	LEFT JOIN {{ ref('TgtMAP_CSE_CRIS_PDCTLks') }} ON 
	LEFT JOIN {{ ref('TgtMAP_CSE_DOCU_DELY_METHLks') }} ON 
)

SELECT * FROM LkpReferences