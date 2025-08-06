{{ config(materialized='view', tags=['XfmChlBusTuAPPFrmExt']) }}

WITH ModNullHandling AS (
	SELECT 
	--Manual
	--DOCU_DELY_METH_C: string[5] = handle_null(DOCU_DELY_METH_C,'99999')
	--PYAD_TYPE_C: string[5]=handle_null(PYAD_TYPE_C,'99999')
	--STAT_X:nullable string[max=20]  =handle_null(STAT_X,'UNKNOWN')
	--ISO_CNTY_C:nullable string[max=2]=handle_null(ISO_CNTY_C,'99')
	--SRCE_SYST_C:nullable string[max=3]=handle_null(SRCE_SYST_C,'NA')
	--TOPUP_AGENT_ID:nullable string[max=20]=handle_null(TOPUP_AGENT_ID,'NA')
	--APPT_PDCT_A:nullable string[max=20] =  handle_null(TOPUP_AMOUNT,'NA')
	--ACCT_QLFY_C:nullable string[max=10] = handle_null(ACCT_QLFY_C,'NA')
	--TU_DOCCOLLECT_METHOD_CAT_ID:nullable string[max=5] = handle_null(TU_DOCCOLLECT_METHOD_CAT_ID,'NA')
	--TU_DOCCOLLECT_OVERSEA_STATE:nullable string[max=100]=handle_null(TU_DOCCOLLECT_OVERSEA_STATE,'-1')
	--APPT_QLFY_C:nullable string[max=5] = handle_null(APPT_QLFY_C,'NA')
	--DOCCOLLECT_BSB:nullable string[max=12] =handle_null(DOCCOLLECT_BSB,'NA')
	--CRIS_PRODUCT_ID:nullable string[max=3] = handle_null(CRIS_PRODUCT_ID,'NA')
	--ACCOUNT_NO: nullable string[max=19] = handle_null(ACCOUNT_NO,'NA')
	--TU_DOCCOLLECT_SUBURB:nullable string[max=35] = handle_null(TU_DOCCOLLECT_SUBURB,'-1')
	SBTY_CODE, HL_APP_PROD_ID, TU_APP_ID, TU_ACCOUNT_ID, TU_DOCCOLLECT_METHOD_CAT_ID, ADRS_TYPE_ID, DOCCOLLECT_BSB, TU_DOCCOLLECT_ADDRESS_LINE_1, TU_DOCCOLLECT_ADDRESS_LINE_2, TU_DOCCOLLECT_SUBURB, STAT_C, TU_DOCCOLLECT_POSTCODE, CNTY_ID, TU_DOCCOLLECT_OVERSEA_STATE, TOPUP_AMOUNT AS APPT_PDCT_A, TOPUP_AGENT_ID, TOPUP_AGENT_NAME, ACCOUNT_NO, CRIS_PRODUCT_ID, ORIG_ETL_D, APPT_QLFY_C, DOCU_DELY_METH_C, PYAD_TYPE_C, STAT_X, ISO_CNTY_C, ACCT_QLFY_C, SRCE_SYST_C, CAMPAIGN_CODE 
	FROM {{ ref('LkpReferences') }}
)

SELECT * FROM ModNullHandling