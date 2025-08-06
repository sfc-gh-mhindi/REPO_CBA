{{ config(materialized='view', tags=['XfmDelFlagPlIntRateJoin']) }}

WITH XfmPlIntRate__ModifyRejectTableRecs AS (
	SELECT
		-- *SRC*: \(20)If Len(Trim(( IF IsNotNull((InPlIntRateXfm.DELETED_KEY_1_VALUE)) THEN (InPlIntRateXfm.DELETED_KEY_1_VALUE) ELSE ""))) > 0 Then 'Y' Else 'N',
		IFF(LEN(TRIM(IFF({{ ref('JnPlIntRate') }}.DELETED_KEY_1_VALUE IS NOT NULL, {{ ref('JnPlIntRate') }}.DELETED_KEY_1_VALUE, ''))) > 0, 'Y', 'N') AS svLoadTargetTable,
		-- *SRC*: \(20)If svLoadTargetTable = 'N' AND Len(Trim(( IF IsNotNull((InPlIntRateXfm.DELETED_KEY_1_VALUE_R)) THEN (InPlIntRateXfm.DELETED_KEY_1_VALUE_R) ELSE ""))) > 0 Then 'Y' Else 'N',
		IFF(svLoadTargetTable = 'N' AND LEN(TRIM(IFF({{ ref('JnPlIntRate') }}.DELETED_KEY_1_VALUE_R IS NOT NULL, {{ ref('JnPlIntRate') }}.DELETED_KEY_1_VALUE_R, ''))) > 0, 'Y', 'N') AS svLoadRejectTable,
		{{ ref('JnPlIntRate') }}.DELETED_TABLE_NAME_R AS DELETED_TABLE_NAME,
		{{ ref('JnPlIntRate') }}.DELETED_KEY_1_R AS DELETED_KEY_1,
		{{ ref('JnPlIntRate') }}.DELETED_KEY_1_VALUE_R AS DELETED_KEY_1_VALUE,
		{{ ref('JnPlIntRate') }}.DELETED_KEY_2_R AS DELETED_KEY_2,
		{{ ref('JnPlIntRate') }}.DELETED_KEY_2_VALUE_R AS DELETED_KEY_2_VALUE,
		{{ ref('JnPlIntRate') }}.DELETED_KEY_3_R AS DELETED_KEY_3,
		{{ ref('JnPlIntRate') }}.DELETED_KEY_3_VALUE_R AS DELETED_KEY_3_VALUE,
		{{ ref('JnPlIntRate') }}.DELETED_KEY_4_R AS DELETED_KEY_4,
		{{ ref('JnPlIntRate') }}.DELETED_KEY_4_VALUE_R AS DELETED_KEY_4_VALUE,
		{{ ref('JnPlIntRate') }}.DELETED_KEY_5_R AS DELETED_KEY_5,
		{{ ref('JnPlIntRate') }}.DELETED_KEY_5_VALUE_R AS DELETED_KEY_5_VALUE
	FROM {{ ref('JnPlIntRate') }}
	WHERE svLoadRejectTable = 'Y'
)

SELECT * FROM XfmPlIntRate__ModifyRejectTableRecs