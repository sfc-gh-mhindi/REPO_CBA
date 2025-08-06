{{ config(materialized='view', tags=['XfmDelFlagHlIntRateJoin']) }}

WITH XfmHlIntRate__ModifyRejectTableRecs AS (
	SELECT
		-- *SRC*: \(20)If Len(Trim(( IF IsNotNull((InHlIntRateXfm.DELETED_KEY_1_VALUE)) THEN (InHlIntRateXfm.DELETED_KEY_1_VALUE) ELSE ""))) > 0 Then 'Y' Else 'N',
		IFF(LEN(TRIM(IFF({{ ref('JnHlIntRate') }}.DELETED_KEY_1_VALUE IS NOT NULL, {{ ref('JnHlIntRate') }}.DELETED_KEY_1_VALUE, ''))) > 0, 'Y', 'N') AS svLoadTargetTable,
		-- *SRC*: \(20)If svLoadTargetTable = 'N' AND Len(Trim(( IF IsNotNull((InHlIntRateXfm.DELETED_KEY_1_VALUE_R)) THEN (InHlIntRateXfm.DELETED_KEY_1_VALUE_R) ELSE ""))) > 0 Then 'Y' Else 'N',
		IFF(svLoadTargetTable = 'N' AND LEN(TRIM(IFF({{ ref('JnHlIntRate') }}.DELETED_KEY_1_VALUE_R IS NOT NULL, {{ ref('JnHlIntRate') }}.DELETED_KEY_1_VALUE_R, ''))) > 0, 'Y', 'N') AS svLoadRejectTable,
		{{ ref('JnHlIntRate') }}.DELETED_TABLE_NAME_R AS DELETED_TABLE_NAME,
		{{ ref('JnHlIntRate') }}.DELETED_KEY_1_R AS DELETED_KEY_1,
		{{ ref('JnHlIntRate') }}.DELETED_KEY_1_VALUE_R AS DELETED_KEY_1_VALUE,
		{{ ref('JnHlIntRate') }}.DELETED_KEY_2_R AS DELETED_KEY_2,
		{{ ref('JnHlIntRate') }}.DELETED_KEY_2_VALUE_R AS DELETED_KEY_2_VALUE,
		{{ ref('JnHlIntRate') }}.DELETED_KEY_3_R AS DELETED_KEY_3,
		{{ ref('JnHlIntRate') }}.DELETED_KEY_3_VALUE_R AS DELETED_KEY_3_VALUE,
		{{ ref('JnHlIntRate') }}.DELETED_KEY_4_R AS DELETED_KEY_4,
		{{ ref('JnHlIntRate') }}.DELETED_KEY_4_VALUE_R AS DELETED_KEY_4_VALUE,
		{{ ref('JnHlIntRate') }}.DELETED_KEY_5_R AS DELETED_KEY_5,
		{{ ref('JnHlIntRate') }}.DELETED_KEY_5_VALUE_R AS DELETED_KEY_5_VALUE
	FROM {{ ref('JnHlIntRate') }}
	WHERE svLoadRejectTable = 'Y'
)

SELECT * FROM XfmHlIntRate__ModifyRejectTableRecs