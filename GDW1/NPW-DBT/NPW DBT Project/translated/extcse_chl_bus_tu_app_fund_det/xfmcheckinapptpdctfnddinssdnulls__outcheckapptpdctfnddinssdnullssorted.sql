{{ config(materialized='view', tags=['ExtCSE_CHL_BUS_TU_APP_FUND_DET']) }}

WITH XfmCheckInApptPdctFnddInssdNulls__OutCheckApptPdctFnddInssdNullsSorted AS (
	SELECT
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((XfmCheckInApptPdctFnddInssdNulls.TU_APP_ID)) THEN (XfmCheckInApptPdctFnddInssdNulls.TU_APP_ID) ELSE ""))) = 0 or Len(Trim(( IF IsNotNull((XfmCheckInApptPdctFnddInssdNulls.TU_APP_FUNDING_DETAIL_ID)) THEN (XfmCheckInApptPdctFnddInssdNulls.TU_APP_FUNDING_DETAIL_ID) ELSE ""))) = 0 or Len(Trim(( IF IsNotNull((XfmCheckInApptPdctFnddInssdNulls.TU_APP_FUNDING_ID)) THEN (XfmCheckInApptPdctFnddInssdNulls.TU_APP_FUNDING_ID) ELSE ""))) = 0 or Len(Trim(( IF IsNotNull((XfmCheckInApptPdctFnddInssdNulls.SBTY_CODE)) THEN (XfmCheckInApptPdctFnddInssdNulls.SBTY_CODE) ELSE ""))) = 0 or Len(Trim(( IF IsNotNull((XfmCheckInApptPdctFnddInssdNulls.HL_APP_PROD_ID)) THEN (XfmCheckInApptPdctFnddInssdNulls.HL_APP_PROD_ID) ELSE ""))) = 0) Then 'REJ7200' Else '',
		IFF(    
	    LEN(TRIM(IFF({{ ref('CpyInApptPdctFnddInssSeqSeq') }}.TU_APP_ID IS NOT NULL, {{ ref('CpyInApptPdctFnddInssSeqSeq') }}.TU_APP_ID, ''))) = 0
	    or LEN(TRIM(IFF({{ ref('CpyInApptPdctFnddInssSeqSeq') }}.TU_APP_FUNDING_DETAIL_ID IS NOT NULL, {{ ref('CpyInApptPdctFnddInssSeqSeq') }}.TU_APP_FUNDING_DETAIL_ID, ''))) = 0
	    or LEN(TRIM(IFF({{ ref('CpyInApptPdctFnddInssSeqSeq') }}.TU_APP_FUNDING_ID IS NOT NULL, {{ ref('CpyInApptPdctFnddInssSeqSeq') }}.TU_APP_FUNDING_ID, ''))) = 0
	    or LEN(TRIM(IFF({{ ref('CpyInApptPdctFnddInssSeqSeq') }}.SBTY_CODE IS NOT NULL, {{ ref('CpyInApptPdctFnddInssSeqSeq') }}.SBTY_CODE, ''))) = 0
	    or LEN(TRIM(IFF({{ ref('CpyInApptPdctFnddInssSeqSeq') }}.HL_APP_PROD_ID IS NOT NULL, {{ ref('CpyInApptPdctFnddInssSeqSeq') }}.HL_APP_PROD_ID, ''))) = 0, 
	    'REJ7200', 
	    ''
	) AS ErrorCode,
		TU_APP_ID,
		FUNDING_DATE,
		TU_APP_FUNDING_DETAIL_ID,
		TU_APP_FUNDING_ID,
		FUNDING_CBA_ACCOUNT_ID,
		FUNDING_NONCBA_BANK_NUMBER,
		FUNDING_NONCBA_INST_NAME,
		FUNDING_NONCBA_INST_ADDRESS,
		FUNDING_NONCBA_BSB,
		FUNDING_NONCBA_ACCOUNT_NUMBER,
		FUNDING_NONCBA_ACCOUNT_NAME,
		FUNDING_BANKCHEQUE_NUMBER,
		FUNDING_BANKCHEQUE_PAYEE,
		FUNDING_METHOD_CAT_ID,
		FUNDING_BANKCHEQUE_CBAACCOUNT,
		PROGRESSIVE_PAYMENT_AMT,
		SBTY_CODE,
		HL_APP_PROD_ID
	FROM {{ ref('CpyInApptPdctFnddInssSeqSeq') }}
	WHERE SUBSTRING(ErrorCode, 1, 3) <> 'REJ'
)

SELECT * FROM XfmCheckInApptPdctFnddInssdNulls__OutCheckApptPdctFnddInssdNullsSorted