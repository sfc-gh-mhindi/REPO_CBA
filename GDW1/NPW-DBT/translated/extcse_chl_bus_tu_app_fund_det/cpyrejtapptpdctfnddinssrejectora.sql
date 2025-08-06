{{ config(materialized='view', tags=['ExtCSE_CHL_BUS_TU_APP_FUND_DET']) }}

WITH CpyRejtApptPdctFnddInssRejectOra AS (
	SELECT
		{{ ref('SrcRejtApptRelRejectOra') }}.TU_APP_ID AS TU_APP_ID_R,
		{{ ref('SrcRejtApptRelRejectOra') }}.FUNDING_DATE AS FUNDING_DATE_R,
		TU_APP_FUNDING_DETAIL_ID,
		{{ ref('SrcRejtApptRelRejectOra') }}.TU_APP_FUNDING_ID AS TU_APP_FUNDING_ID_R,
		{{ ref('SrcRejtApptRelRejectOra') }}.FUNDING_CBA_ACCOUNT_ID AS FUNDING_CBA_ACCOUNT_ID_R,
		{{ ref('SrcRejtApptRelRejectOra') }}.FUNDING_NONCBA_INSTITUTION_ID AS FUNDING_NONCBA_BANK_NUMBER_R,
		{{ ref('SrcRejtApptRelRejectOra') }}.FUNDING_NONCBA_INST_NAME AS FUNDING_NONCBA_INST_NAME_R,
		{{ ref('SrcRejtApptRelRejectOra') }}.FUNDING_NONCBA_INST_ADDRESS AS FUNDING_NONCBA_INST_ADDRESS_R,
		{{ ref('SrcRejtApptRelRejectOra') }}.FUNDING_NONCBA_BSB AS FUNDING_NONCBA_BSB_R,
		{{ ref('SrcRejtApptRelRejectOra') }}.FUNDING_NONCBA_ACCOUNT_NUMBER AS FUNDING_NONCBA_ACCOUNT_NUMBER_R,
		{{ ref('SrcRejtApptRelRejectOra') }}.FUNDING_NONCBA_ACCOUNT_NAME AS FUNDING_NONCBA_ACCOUNT_NAME_R,
		{{ ref('SrcRejtApptRelRejectOra') }}.FUNDING_BANKCHEQUE_NUMBER AS FUNDING_BANKCHEQUE_NUMBER_R,
		{{ ref('SrcRejtApptRelRejectOra') }}.FUNDING_BANKCHEQUE_PAYEE AS FUNDING_BANKCHEQUE_PAYEE_R,
		{{ ref('SrcRejtApptRelRejectOra') }}.FUNDING_METHOD_CAT_ID AS FUNDING_METHOD_CAT_ID_R,
		{{ ref('SrcRejtApptRelRejectOra') }}.FUNDING_BANKCHEQUE_CBAACCOUNT AS FUNDING_BANKCHEQUE_CBAACCOUNT_R,
		{{ ref('SrcRejtApptRelRejectOra') }}.PROGRESSIVE_PAYMENT_AMT AS PROGRESSIVE_PAYMENT_AMT_R,
		{{ ref('SrcRejtApptRelRejectOra') }}.SBTY_CODE AS SBTY_CODE_R,
		{{ ref('SrcRejtApptRelRejectOra') }}.HL_APP_PROD_ID AS HL_APP_PROD_ID_R,
		{{ ref('SrcRejtApptRelRejectOra') }}.ORIG_ETL_D AS ORIG_ETL_D_R
	FROM {{ ref('SrcRejtApptRelRejectOra') }}
)

SELECT * FROM CpyRejtApptPdctFnddInssRejectOra