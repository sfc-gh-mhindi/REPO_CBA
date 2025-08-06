{{ config(materialized='view', tags=['XfmTu_ApptPdctFnddInssFrmExt']) }}

WITH LkpReferences AS (
	SELECT
		{{ ref('CpyRename') }}.TU_APP_ID,
		{{ ref('CpyRename') }}.FUNDING_DATE,
		{{ ref('CpyRename') }}.TU_APP_FUNDING_DETAIL_ID,
		{{ ref('CpyRename') }}.TU_APP_FUNDING_ID,
		{{ ref('CpyRename') }}.FUNDING_CBA_ACCOUNT_ID,
		{{ ref('CpyRename') }}.INSN_ID AS FUNDING_NONCBA_BANK_NUMBER,
		{{ ref('CpyRename') }}.FUNDING_NONCBA_INST_NAME,
		{{ ref('CpyRename') }}.FUNDING_NONCBA_INST_ADDRESS,
		{{ ref('CpyRename') }}.FUNDING_NONCBA_BSB,
		{{ ref('CpyRename') }}.FUNDING_NONCBA_ACCOUNT_NUMBER,
		{{ ref('CpyRename') }}.FUNDING_NONCBA_ACCOUNT_NAME,
		{{ ref('CpyRename') }}.FUNDING_BANKCHEQUE_NUMBER,
		{{ ref('CpyRename') }}.FUNDING_BANKCHEQUE_PAYEE,
		{{ ref('CpyRename') }}.FNDD_METH_CAT_ID AS FUNDING_METHOD_CAT_ID,
		{{ ref('CpyRename') }}.FUNDING_BANKCHEQUE_CBAACCOUNT,
		{{ ref('CpyRename') }}.PROGRESSIVE_PAYMENT_AMT,
		{{ ref('CpyRename') }}.SBTY_CODE,
		{{ ref('CpyRename') }}.HL_APP_PROD_ID,
		{{ ref('CpyRename') }}.ORIG_ETL_D,
		{{ ref('TgtMAP_CSE_APPT_QLFYLks') }}.APPT_QLFY_C,
		{{ ref('TgtMAP_CSE_FNDD_METHLks') }}.FNDD_INSS_METH_C,
		{{ ref('TgtMAP_CSE_CMPE_IDNNLks') }}.CMPE_I,
		OutApptPdctFnddInssPremapDS.TU_APP_ID,
		OutApptPdctFnddInssPremapDS.FUNDING_DATE,
		OutApptPdctFnddInssPremapDS.TU_APP_FUNDING_DETAIL_ID,
		OutApptPdctFnddInssPremapDS.TU_APP_FUNDING_ID,
		OutApptPdctFnddInssPremapDS.FUNDING_CBA_ACCOUNT_ID,
		OutApptPdctFnddInssPremapDS.FUNDING_NONCBA_INSTITUTION_ID AS INSN_ID,
		OutApptPdctFnddInssPremapDS.FUNDING_NONCBA_INST_NAME,
		OutApptPdctFnddInssPremapDS.FUNDING_NONCBA_INST_ADDRESS,
		OutApptPdctFnddInssPremapDS.FUNDING_NONCBA_BSB,
		OutApptPdctFnddInssPremapDS.FUNDING_NONCBA_ACCOUNT_NUMBER,
		OutApptPdctFnddInssPremapDS.FUNDING_NONCBA_ACCOUNT_NAME,
		OutApptPdctFnddInssPremapDS.FUNDING_BANKCHEQUE_NUMBER,
		OutApptPdctFnddInssPremapDS.FUNDING_BANKCHEQUE_PAYEE,
		OutApptPdctFnddInssPremapDS.FUNDING_METHOD_CAT_ID AS FNDD_METH_CAT_ID,
		OutApptPdctFnddInssPremapDS.FUNDING_BANKCHEQUE_CBAACCOUNT,
		OutApptPdctFnddInssPremapDS.PROGRESSIVE_PAYMENT_AMT,
		OutApptPdctFnddInssPremapDS.SBTY_CODE,
		OutApptPdctFnddInssPremapDS.HL_APP_PROD_ID,
		OutApptPdctFnddInssPremapDS.ORIG_ETL_D
	FROM {{ ref('CpyRename') }}
	LEFT JOIN {{ ref('TgtMAP_CSE_APPT_QLFYLks') }} ON 
	LEFT JOIN {{ ref('TgtMAP_CSE_CMPE_IDNNLks') }} ON 
	LEFT JOIN {{ ref('TgtMAP_CSE_FNDD_METHLks') }} ON 
)

SELECT * FROM LkpReferences