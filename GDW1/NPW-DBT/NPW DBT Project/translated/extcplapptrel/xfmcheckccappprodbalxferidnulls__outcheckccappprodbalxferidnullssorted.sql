{{ config(materialized='view', tags=['ExtCplApptRel']) }}

WITH XfmCheckCCAppProdBalXferIdNulls__OutCheckCCAppProdBalXferIdNullsSorted AS (
	SELECT
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((XfmCheckCCAppProdBalXferIdNulls.APPT_I)) THEN (XfmCheckCCAppProdBalXferIdNulls.APPT_I) ELSE ""))) = 0) Then 'REJ789' else  if (Len(Trim(( IF IsNotNull((XfmCheckCCAppProdBalXferIdNulls.RELD_APPT_I)) THEN (XfmCheckCCAppProdBalXferIdNulls.RELD_APPT_I) ELSE ""))) = 0) Then 'REJ790' Else '',
		IFF(LEN(TRIM(IFF({{ ref('CpyCCAppProdBalXferSeq') }}.APPT_I IS NOT NULL, {{ ref('CpyCCAppProdBalXferSeq') }}.APPT_I, ''))) = 0, 'REJ789', IFF(LEN(TRIM(IFF({{ ref('CpyCCAppProdBalXferSeq') }}.RELD_APPT_I IS NOT NULL, {{ ref('CpyCCAppProdBalXferSeq') }}.RELD_APPT_I, ''))) = 0, 'REJ790', '')) AS ErrorCode,
		LOAN_SUBTYPE_CODE,
		APPT_I,
		REL_TYPE_C,
		RELD_APPT_I
	FROM {{ ref('CpyCCAppProdBalXferSeq') }}
	WHERE SUBSTRING(ErrorCode, 1, 3) <> 'REJ'
)

SELECT * FROM XfmCheckCCAppProdBalXferIdNulls__OutCheckCCAppProdBalXferIdNullsSorted