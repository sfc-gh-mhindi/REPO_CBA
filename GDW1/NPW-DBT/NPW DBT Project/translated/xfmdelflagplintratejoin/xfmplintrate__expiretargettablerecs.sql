{{ config(materialized='view', tags=['XfmDelFlagPlIntRateJoin']) }}

WITH XfmPlIntRate__ExpireTargetTableRecs AS (
	SELECT
		-- *SRC*: \(20)If Len(Trim(( IF IsNotNull((InPlIntRateXfm.DELETED_KEY_1_VALUE)) THEN (InPlIntRateXfm.DELETED_KEY_1_VALUE) ELSE ""))) > 0 Then 'Y' Else 'N',
		IFF(LEN(TRIM(IFF({{ ref('JnPlIntRate') }}.DELETED_KEY_1_VALUE IS NOT NULL, {{ ref('JnPlIntRate') }}.DELETED_KEY_1_VALUE, ''))) > 0, 'Y', 'N') AS svLoadTargetTable,
		-- *SRC*: \(20)If svLoadTargetTable = 'N' AND Len(Trim(( IF IsNotNull((InPlIntRateXfm.DELETED_KEY_1_VALUE_R)) THEN (InPlIntRateXfm.DELETED_KEY_1_VALUE_R) ELSE ""))) > 0 Then 'Y' Else 'N',
		IFF(svLoadTargetTable = 'N' AND LEN(TRIM(IFF({{ ref('JnPlIntRate') }}.DELETED_KEY_1_VALUE_R IS NOT NULL, {{ ref('JnPlIntRate') }}.DELETED_KEY_1_VALUE_R, ''))) > 0, 'Y', 'N') AS svLoadRejectTable,
		DELETED_TABLE_NAME,
		DELETED_KEY_1,
		DELETED_KEY_1_VALUE,
		DELETED_KEY_2,
		DELETED_KEY_2_VALUE,
		DELETED_KEY_3,
		DELETED_KEY_3_VALUE,
		DELETED_KEY_4,
		DELETED_KEY_4_VALUE,
		DELETED_KEY_5,
		DELETED_KEY_5_VALUE
	FROM {{ ref('JnPlIntRate') }}
	WHERE svLoadTargetTable = 'Y'
)

SELECT * FROM XfmPlIntRate__ExpireTargetTableRecs