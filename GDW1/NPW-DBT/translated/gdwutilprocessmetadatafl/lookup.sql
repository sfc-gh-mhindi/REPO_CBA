{{ config(materialized='view', tags=['GDWUtilProcessMetaDataFL']) }}

WITH Lookup AS (
	SELECT
		{{ ref('Last_Record') }}.InputRows,
		{{ ref('Last_Record') }}.InputBytes,
		{{ ref('Last_Record') }}.InputRowsRejected,
		{{ ref('Last_Record') }}.DuplicateRows,
		{{ ref('Last_Record') }}.FieldConversionErrors,
		{{ ref('Last_Record') }}.DuplicateIndexErrors,
		{{ ref('Last_Record') }}.RowsWritten,
		{{ ref('Last_Record') }}.RowsInStagingTable,
		{{ ref('Last_Record') }}.RowsInsertedFromStagingTable,
		{{ ref('Last_Record') }}.TotalRowsRejected,
		{{ ref('Last_Record') }}.Timestamp,
		{{ ref('Lkup_UTIL_PROS_ISAC') }}.SRCE_LOAD_CNT_SUM,
		{{ ref('Last_Record') }}.BTCH_KEY_I,
		{{ ref('Last_Record') }}.TRGT_M
	FROM {{ ref('Last_Record') }}
	LEFT JOIN {{ ref('Lkup_UTIL_PROS_ISAC') }} ON {{ ref('Last_Record') }}.BTCH_KEY_I = {{ ref('Lkup_UTIL_PROS_ISAC') }}.BTCH_KEY_I
	AND {{ ref('Last_Record') }}.TRGT_M = {{ ref('Lkup_UTIL_PROS_ISAC') }}.TRGT_M
)

SELECT * FROM Lookup