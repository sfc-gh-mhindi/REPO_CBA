{{ config(materialized='view', tags=['ExtPlFeePlMargin']) }}

WITH XfmCheckPlFeeIdNulls__OutCheckPlFeeIdNullsSorted AS (
	SELECT
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((XfmCheckPlFeeIdNulls.PL_FEE_ID)) THEN (XfmCheckPlFeeIdNulls.PL_FEE_ID) ELSE ""))) = 0) Then 'REJ1001' Else ( If ((Len(Trim(( IF IsNotNull((XfmCheckPlFeeIdNulls.PL_MARGIN_PL_MARGIN_ID)) THEN (XfmCheckPlFeeIdNulls.PL_MARGIN_PL_MARGIN_ID) ELSE ""))) = 0) and XfmCheckPlFeeIdNulls.PL_MARGIN_FOUND_FLAG = 'Y') then 'REJ1002' else ''),
		IFF(LEN(TRIM(IFF({{ ref('Cpy_NoOp') }}.PL_FEE_ID IS NOT NULL, {{ ref('Cpy_NoOp') }}.PL_FEE_ID, ''))) = 0, 'REJ1001', IFF(LEN(TRIM(IFF({{ ref('Cpy_NoOp') }}.PL_MARGIN_PL_MARGIN_ID IS NOT NULL, {{ ref('Cpy_NoOp') }}.PL_MARGIN_PL_MARGIN_ID, ''))) = 0 AND {{ ref('Cpy_NoOp') }}.PL_MARGIN_FOUND_FLAG = 'Y', 'REJ1002', '')) AS ErrorCode,
		PL_FEE_ID,
		PL_APP_PROD_ID,
		PL_FEE_PL_FEE_ID,
		PL_FEE_ADD_TO_TOTAL_FLAG,
		PL_FEE_FEE_AMT,
		PL_FEE_BASE_FEE_AMT,
		PL_FEE_PAY_FEE_AT_FUNDING_FLAG,
		PL_FEE_START_DATE,
		PL_FEE_PL_CAPITALIS_FEE_CAT_ID,
		PL_FEE_FEE_SCREEN_DESC,
		PL_FEE_FEE_DESC,
		PL_FEE_CASS_FEE_CODE,
		PL_FEE_CASS_FEE_KEY,
		PL_FEE_TOTAL_FEE_AMT,
		PL_FEE_PL_APP_PROD_ID,
		PL_MARGIN_PL_MARGIN_ID,
		PL_MARGIN_MARGIN_AMT,
		PL_MARGIN_PL_FEE_ID,
		PL_MARGIN_PL_INT_RATE_ID,
		PL_MARGIN_MARGIN_REASON_CAT_ID,
		PL_MARGIN_PL_APP_PROD_ID,
		PL_FEE_FOUND_FLAG,
		PL_MARGIN_FOUND_FLAG
	FROM {{ ref('Cpy_NoOp') }}
	WHERE SUBSTRING(ErrorCode, 1, 3) <> 'REJ'
)

SELECT * FROM XfmCheckPlFeeIdNulls__OutCheckPlFeeIdNullsSorted