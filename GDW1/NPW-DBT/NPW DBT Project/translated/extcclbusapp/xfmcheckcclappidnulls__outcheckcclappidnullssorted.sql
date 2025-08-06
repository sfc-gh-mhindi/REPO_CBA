{{ config(materialized='view', tags=['ExtCclBusApp']) }}

WITH XfmCheckCclAppIdNulls__OutCheckCclAppIdNullsSorted AS (
	SELECT
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((XfmCheckCclAppIdNulls.CCL_APP_ID)) THEN (XfmCheckCclAppIdNulls.CCL_APP_ID) ELSE ""))) = 0) Then 'REJ3000' Else '',
		IFF(LEN(TRIM(IFF({{ ref('CpyCclAppSeq') }}.CCL_APP_ID IS NOT NULL, {{ ref('CpyCclAppSeq') }}.CCL_APP_ID, ''))) = 0, 'REJ3000', '') AS ErrorCode,
		CCL_APP_ID,
		CCL_APP_CAT_ID,
		CCL_FORM_CAT_ID,
		TOTAL_PERSONAL_FAC_AMT,
		TOTAL_EQUIPMENTFINANCE_FAC_AMT,
		TOTAL_COMMERCIAL_FAC_AMT,
		TOPUP_APP_ID,
		AF_PRIMARY_INDUSTRY_ID,
		AD_TUC_AMT,
		COMMISSION_AMT,
		BROKER_REFERAL_FLAG,
		CARNELL_EXPOSURE_AMT,
		CARNELL_EXPOSURE_AMT_DATE,
		CARNELL_OVERRIDE_COV_ASSESSMNT,
		CARNELL_OVERRIDE_REASON_CAT_ID,
		CARNELL_SHORT_DEFAULT_OVERRIDE,
		'Y' AS SRCE_REC
	FROM {{ ref('CpyCclAppSeq') }}
	WHERE SUBSTRING(ErrorCode, 1, 3) <> 'REJ'
)

SELECT * FROM XfmCheckCclAppIdNulls__OutCheckCclAppIdNullsSorted