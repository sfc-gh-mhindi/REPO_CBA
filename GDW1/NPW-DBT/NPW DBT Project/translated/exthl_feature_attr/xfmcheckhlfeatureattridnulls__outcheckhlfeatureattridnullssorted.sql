{{ config(materialized='view', tags=['ExtHL_FEATURE_ATTR']) }}

WITH XfmCheckHlFeatureAttrIdNulls__OutCheckHlFeatureAttrIdNullsSorted AS (
	SELECT
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((XfmCheckHlFeatureAttriIdNulls.HL_FEATURE_ATTR_ID)) THEN (XfmCheckHlFeatureAttriIdNulls.HL_FEATURE_ATTR_ID) ELSE ""))) = 0) Then 'REJ4001' Else '',
		IFF(LEN(TRIM(IFF({{ ref('CpyHlFeatureAttrSeq') }}.HL_FEATURE_ATTR_ID IS NOT NULL, {{ ref('CpyHlFeatureAttrSeq') }}.HL_FEATURE_ATTR_ID, ''))) = 0, 'REJ4001', '') AS ErrorCode,
		HL_FEATURE_ATTR_ID,
		HL_FEATURE_TERM,
		HL_FEATURE_AMOUNT,
		HL_FEATURE_BALANCE,
		HL_FEATURE_FEE,
		HL_FEATURE_SPEC_REPAY,
		HL_FEATURE_EST_INT_AMT,
		HL_FEATURE_DATE,
		HL_FEATURE_COMMENT,
		HL_FEATURE_CAT_ID,
		HL_APP_PROD_ID
	FROM {{ ref('CpyHlFeatureAttrSeq') }}
	WHERE SUBSTRING(ErrorCode, 1, 3) <> 'REJ'
)

SELECT * FROM XfmCheckHlFeatureAttrIdNulls__OutCheckHlFeatureAttrIdNullsSorted