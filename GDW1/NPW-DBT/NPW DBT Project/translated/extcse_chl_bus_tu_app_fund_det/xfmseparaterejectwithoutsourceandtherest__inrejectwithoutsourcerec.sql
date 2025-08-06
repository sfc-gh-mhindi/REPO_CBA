{{ config(materialized='view', tags=['ExtCSE_CHL_BUS_TU_APP_FUND_DET']) }}

WITH XfmSeparateRejectWithoutSourceAndTheRest__InRejectWithoutSourceRec AS (
	SELECT
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((XfmSeparateRejects.TU_APP_FUNDING_DETAIL_ID)) THEN (XfmSeparateRejects.TU_APP_FUNDING_DETAIL_ID) ELSE ""))) = 0) Then 'R' Else 'S',
		IFF(LEN(TRIM(IFF({{ ref('JoinSrcSortReject') }}.TU_APP_FUNDING_DETAIL_ID IS NOT NULL, {{ ref('JoinSrcSortReject') }}.TU_APP_FUNDING_DETAIL_ID, ''))) = 0, 'R', 'S') AS DeltaFlag,
		{{ ref('JoinSrcSortReject') }}.TU_APP_ID_R AS TU_APP_ID,
		{{ ref('JoinSrcSortReject') }}.FUNDING_DATE_R AS FUNDING_DATE,
		{{ ref('JoinSrcSortReject') }}.TU_APP_FUNDING_DETAIL_ID_R AS TU_APP_FUNDING_DETAIL_ID,
		{{ ref('JoinSrcSortReject') }}.TU_APP_FUNDING_ID_R AS TU_APP_FUNDING_ID,
		{{ ref('JoinSrcSortReject') }}.FUNDING_CBA_ACCOUNT_ID_R AS FUNDING_CBA_ACCOUNT_ID,
		{{ ref('JoinSrcSortReject') }}.FUNDING_NONCBA_ACCOUNT_NUMBER_R AS FUNDING_NONCBA_BANK_NUMBER,
		{{ ref('JoinSrcSortReject') }}.FUNDING_NONCBA_INST_NAME_R AS FUNDING_NONCBA_INST_NAME,
		{{ ref('JoinSrcSortReject') }}.FUNDING_NONCBA_INST_ADDRESS_R AS FUNDING_NONCBA_INST_ADDRESS,
		{{ ref('JoinSrcSortReject') }}.FUNDING_NONCBA_BSB_R AS FUNDING_NONCBA_BSB,
		{{ ref('JoinSrcSortReject') }}.FUNDING_NONCBA_ACCOUNT_NUMBER_R AS FUNDING_NONCBA_ACCOUNT_NUMBER,
		{{ ref('JoinSrcSortReject') }}.FUNDING_NONCBA_ACCOUNT_NAME_R AS FUNDING_NONCBA_ACCOUNT_NAME,
		{{ ref('JoinSrcSortReject') }}.FUNDING_BANKCHEQUE_NUMBER_R AS FUNDING_BANKCHEQUE_NUMBER,
		{{ ref('JoinSrcSortReject') }}.FUNDING_BANKCHEQUE_PAYEE_R AS FUNDING_BANKCHEQUE_PAYEE,
		{{ ref('JoinSrcSortReject') }}.FUNDING_METHOD_CAT_ID_R AS FUNDING_METHOD_CAT_ID,
		{{ ref('JoinSrcSortReject') }}.FUNDING_BANKCHEQUE_CBAACCOUNT_R AS FUNDING_BANKCHEQUE_CBAACCOUNT,
		{{ ref('JoinSrcSortReject') }}.PROGRESSIVE_PAYMENT_AMT_R AS PROGRESSIVE_PAYMENT_AMT,
		{{ ref('JoinSrcSortReject') }}.SBTY_CODE_R AS SBTY_CODE,
		{{ ref('JoinSrcSortReject') }}.HL_APP_PROD_ID_R AS HL_APP_PROD_ID,
		{{ ref('JoinSrcSortReject') }}.ORIG_ETL_D_R AS ORIG_ETL_D
	FROM {{ ref('JoinSrcSortReject') }}
	WHERE DeltaFlag = 'R'
)

SELECT * FROM XfmSeparateRejectWithoutSourceAndTheRest__InRejectWithoutSourceRec