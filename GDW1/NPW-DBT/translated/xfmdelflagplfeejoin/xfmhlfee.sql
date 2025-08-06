{{ config(materialized='view', tags=['XfmDelFlagPlFeeJoin']) }}

WITH XfmHlFee AS (
	SELECT
		-- *SRC*: \(20)If Len(Trim(( IF IsNotNull((InHlFeeXfm.DELETED_KEY_1_VALUE)) THEN (InHlFeeXfm.DELETED_KEY_1_VALUE) ELSE ""))) = 0 AND Len(Trim(( IF IsNotNull((InHlFeeXfm.DELETED_KEY_1_VALUE_R)) THEN (InHlFeeXfm.DELETED_KEY_1_VALUE_R) ELSE ""))) > 0 Then 'Y' Else 'N',
		IFF(LEN(TRIM(IFF({{ ref('JnHlFee') }}.DELETED_KEY_1_VALUE IS NOT NULL, {{ ref('JnHlFee') }}.DELETED_KEY_1_VALUE, ''))) = 0 AND LEN(TRIM(IFF({{ ref('JnHlFee') }}.DELETED_KEY_1_VALUE_R IS NOT NULL, {{ ref('JnHlFee') }}.DELETED_KEY_1_VALUE_R, ''))) > 0, 'Y', 'N') AS svLoadRejectTable,
		{{ ref('JnHlFee') }}.DELETED_TABLE_NAME_R AS DELETED_TABLE_NAME,
		{{ ref('JnHlFee') }}.DELETED_KEY_1_R AS DELETED_KEY_1,
		{{ ref('JnHlFee') }}.DELETED_KEY_1_VALUE_R AS DELETED_KEY_1_VALUE,
		{{ ref('JnHlFee') }}.DELETED_KEY_2_R AS DELETED_KEY_2,
		{{ ref('JnHlFee') }}.DELETED_KEY_2_VALUE_R AS DELETED_KEY_2_VALUE,
		{{ ref('JnHlFee') }}.DELETED_KEY_3_R AS DELETED_KEY_3,
		{{ ref('JnHlFee') }}.DELETED_KEY_3_VALUE_R AS DELETED_KEY_3_VALUE,
		{{ ref('JnHlFee') }}.DELETED_KEY_4_R AS DELETED_KEY_4,
		{{ ref('JnHlFee') }}.DELETED_KEY_4_VALUE_R AS DELETED_KEY_4_VALUE,
		{{ ref('JnHlFee') }}.DELETED_KEY_5_R AS DELETED_KEY_5,
		{{ ref('JnHlFee') }}.DELETED_KEY_5_VALUE_R AS DELETED_KEY_5_VALUE
	FROM {{ ref('JnHlFee') }}
	WHERE svLoadRejectTable = 'Y'
)

SELECT * FROM XfmHlFee