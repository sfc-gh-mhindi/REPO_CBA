{{ config(materialized='view', tags=['ExtCSE_CHL_BUS_TU_APP_FUND_DET']) }}

WITH JoinSrcSortReject AS (
	SELECT
		{{ ref('XfmCheckInApptPdctFnddInssdNulls__OutCheckApptPdctFnddInssdNullsSorted') }}.TU_APP_ID,
		{{ ref('XfmCheckInApptPdctFnddInssdNulls__OutCheckApptPdctFnddInssdNullsSorted') }}.FUNDING_DATE,
		{{ ref('XfmCheckInApptPdctFnddInssdNulls__OutCheckApptPdctFnddInssdNullsSorted') }}.TU_APP_FUNDING_DETAIL_ID,
		{{ ref('XfmCheckInApptPdctFnddInssdNulls__OutCheckApptPdctFnddInssdNullsSorted') }}.TU_APP_FUNDING_ID,
		{{ ref('XfmCheckInApptPdctFnddInssdNulls__OutCheckApptPdctFnddInssdNullsSorted') }}.FUNDING_CBA_ACCOUNT_ID,
		{{ ref('XfmCheckInApptPdctFnddInssdNulls__OutCheckApptPdctFnddInssdNullsSorted') }}.FUNDING_NONCBA_BANK_NUMBER,
		{{ ref('XfmCheckInApptPdctFnddInssdNulls__OutCheckApptPdctFnddInssdNullsSorted') }}.FUNDING_NONCBA_INST_NAME,
		{{ ref('XfmCheckInApptPdctFnddInssdNulls__OutCheckApptPdctFnddInssdNullsSorted') }}.FUNDING_NONCBA_INST_ADDRESS,
		{{ ref('XfmCheckInApptPdctFnddInssdNulls__OutCheckApptPdctFnddInssdNullsSorted') }}.FUNDING_NONCBA_BSB,
		{{ ref('XfmCheckInApptPdctFnddInssdNulls__OutCheckApptPdctFnddInssdNullsSorted') }}.FUNDING_NONCBA_ACCOUNT_NUMBER,
		{{ ref('XfmCheckInApptPdctFnddInssdNulls__OutCheckApptPdctFnddInssdNullsSorted') }}.FUNDING_NONCBA_ACCOUNT_NAME,
		{{ ref('XfmCheckInApptPdctFnddInssdNulls__OutCheckApptPdctFnddInssdNullsSorted') }}.FUNDING_BANKCHEQUE_NUMBER,
		{{ ref('XfmCheckInApptPdctFnddInssdNulls__OutCheckApptPdctFnddInssdNullsSorted') }}.FUNDING_BANKCHEQUE_PAYEE,
		{{ ref('XfmCheckInApptPdctFnddInssdNulls__OutCheckApptPdctFnddInssdNullsSorted') }}.FUNDING_METHOD_CAT_ID,
		{{ ref('XfmCheckInApptPdctFnddInssdNulls__OutCheckApptPdctFnddInssdNullsSorted') }}.FUNDING_BANKCHEQUE_CBAACCOUNT,
		{{ ref('XfmCheckInApptPdctFnddInssdNulls__OutCheckApptPdctFnddInssdNullsSorted') }}.PROGRESSIVE_PAYMENT_AMT,
		{{ ref('XfmCheckInApptPdctFnddInssdNulls__OutCheckApptPdctFnddInssdNullsSorted') }}.SBTY_CODE,
		{{ ref('XfmCheckInApptPdctFnddInssdNulls__OutCheckApptPdctFnddInssdNullsSorted') }}.HL_APP_PROD_ID,
		{{ ref('CpyRejtApptPdctFnddInssRejectOra') }}.TU_APP_ID_R,
		{{ ref('CpyRejtApptPdctFnddInssRejectOra') }}.FUNDING_DATE_R,
		{{ ref('CpyRejtApptPdctFnddInssRejectOra') }}.TU_APP_FUNDING_DETAIL_ID AS TU_APP_FUNDING_DETAIL_ID_R,
		{{ ref('CpyRejtApptPdctFnddInssRejectOra') }}.TU_APP_FUNDING_ID_R,
		{{ ref('CpyRejtApptPdctFnddInssRejectOra') }}.FUNDING_CBA_ACCOUNT_ID_R,
		{{ ref('CpyRejtApptPdctFnddInssRejectOra') }}.FUNDING_NONCBA_BANK_NUMBER_R,
		{{ ref('CpyRejtApptPdctFnddInssRejectOra') }}.FUNDING_NONCBA_INST_NAME_R,
		{{ ref('CpyRejtApptPdctFnddInssRejectOra') }}.FUNDING_NONCBA_INST_ADDRESS_R,
		{{ ref('CpyRejtApptPdctFnddInssRejectOra') }}.FUNDING_NONCBA_BSB_R,
		{{ ref('CpyRejtApptPdctFnddInssRejectOra') }}.FUNDING_NONCBA_ACCOUNT_NUMBER_R,
		{{ ref('CpyRejtApptPdctFnddInssRejectOra') }}.FUNDING_NONCBA_ACCOUNT_NAME_R,
		{{ ref('CpyRejtApptPdctFnddInssRejectOra') }}.FUNDING_BANKCHEQUE_NUMBER_R,
		{{ ref('CpyRejtApptPdctFnddInssRejectOra') }}.FUNDING_BANKCHEQUE_PAYEE_R,
		{{ ref('CpyRejtApptPdctFnddInssRejectOra') }}.FUNDING_METHOD_CAT_ID_R,
		{{ ref('CpyRejtApptPdctFnddInssRejectOra') }}.FUNDING_BANKCHEQUE_CBAACCOUNT_R,
		{{ ref('CpyRejtApptPdctFnddInssRejectOra') }}.PROGRESSIVE_PAYMENT_AMT_R,
		{{ ref('CpyRejtApptPdctFnddInssRejectOra') }}.SBTY_CODE_R,
		{{ ref('CpyRejtApptPdctFnddInssRejectOra') }}.HL_APP_PROD_ID_R,
		{{ ref('CpyRejtApptPdctFnddInssRejectOra') }}.ORIG_ETL_D_R
	FROM {{ ref('XfmCheckInApptPdctFnddInssdNulls__OutCheckApptPdctFnddInssdNullsSorted') }}
	OUTER JOIN {{ ref('CpyRejtApptPdctFnddInssRejectOra') }} ON {{ ref('XfmCheckInApptPdctFnddInssdNulls__OutCheckApptPdctFnddInssdNullsSorted') }}.TU_APP_FUNDING_DETAIL_ID = {{ ref('CpyRejtApptPdctFnddInssRejectOra') }}.TU_APP_FUNDING_DETAIL_ID
)

SELECT * FROM JoinSrcSortReject