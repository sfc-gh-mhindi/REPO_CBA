{{ config(materialized='view', tags=['GDWUtilProcessMetaDataFL']) }}

WITH Find_MetaData AS (
	SELECT
		-- *SRC*: Trim(In_Record.Record),
		TRIM({{ ref('Runstream_TableName_ProsDate_RPT') }}.Record) AS TrimmedRecord,
		-- *SRC*: \(20)If @INROWNUM = 1 then TrimmedRecord[1, 19] else Timestamp,
		IFF(@INROWNUM = 1, SUBSTRING(TrimmedRecord, 1, 19), Timestamp) AS Timestamp,
		-- *SRC*: \(20)If TrimmedRecord[1, 11] = 'Input rows:' Then Convert(',', '', Field(TrimmedRecord, ':', 2)) Else InputRows,
		IFF(SUBSTRING(TrimmedRecord, 1, 11) = 'Input rows:', CONVERT(',', '', FIELD(TrimmedRecord, ':', 2)), InputRows) AS InputRows,
		-- *SRC*: \(20)If TrimmedRecord[1, 12] = 'Input bytes:' Then Convert(',', '', Field(TrimmedRecord, ':', 2)) Else InputBytes,
		IFF(SUBSTRING(TrimmedRecord, 1, 12) = 'Input bytes:', CONVERT(',', '', FIELD(TrimmedRecord, ':', 2)), InputBytes) AS InputBytes,
		-- *SRC*: \(20)If TrimmedRecord[1, 20] = 'Input rows rejected:' Then Convert(',', '', Field(TrimmedRecord, ':', 2)) Else InputRowsRejected,
		IFF(SUBSTRING(TrimmedRecord, 1, 20) = 'Input rows rejected:', CONVERT(',', '', FIELD(TrimmedRecord, ':', 2)), InputRowsRejected) AS InputRowsRejected,
		-- *SRC*: \(20)If TrimmedRecord[1, 15] = 'Duplicate rows:' Then Convert(',', '', Field(TrimmedRecord, ':', 2)) Else DuplicateRows,
		IFF(SUBSTRING(TrimmedRecord, 1, 15) = 'Duplicate rows:', CONVERT(',', '', FIELD(TrimmedRecord, ':', 2)), DuplicateRows) AS DuplicateRows,
		-- *SRC*: \(20)If TrimmedRecord[1, 24] = 'Field conversion errors:' Then Convert(',', '', Field(TrimmedRecord, ':', 2)) Else FieldConversionErrors,
		IFF(SUBSTRING(TrimmedRecord, 1, 24) = 'Field conversion errors:', CONVERT(',', '', FIELD(TrimmedRecord, ':', 2)), FieldConversionErrors) AS FieldConversionErrors,
		-- *SRC*: \(20)If TrimmedRecord[1, 23] = 'Duplicate index errors:' Then Convert(',', '', Field(TrimmedRecord, ':', 2)) Else DuplicateIndexErrors,
		IFF(SUBSTRING(TrimmedRecord, 1, 23) = 'Duplicate index errors:', CONVERT(',', '', FIELD(TrimmedRecord, ':', 2)), DuplicateIndexErrors) AS DuplicateIndexErrors,
		-- *SRC*: \(20)If TrimmedRecord[1, 13] = 'Rows written:' Then Convert(',', '', Field(TrimmedRecord, ':', 2)) Else RowsWritten,
		IFF(SUBSTRING(TrimmedRecord, 1, 13) = 'Rows written:', CONVERT(',', '', FIELD(TrimmedRecord, ':', 2)), RowsWritten) AS RowsWritten,
		-- *SRC*: \(20)If TrimmedRecord[1, 21] = 'Rows in staging table' Then Convert(',', '', Field(TrimmedRecord, 'e', 2)) Else RowsInStagingTable,
		IFF(SUBSTRING(TrimmedRecord, 1, 21) = 'Rows in staging table', CONVERT(',', '', FIELD(TrimmedRecord, 'e', 2)), RowsInStagingTable) AS RowsInStagingTable,
		-- *SRC*: \(20)If TrimmedRecord[1, 33] = 'Rows inserted from staging table:' Then Convert(',', '', Field(In_Record.Record, ':', 2)) Else RowsInsertedFromStagingTable,
		IFF(SUBSTRING(TrimmedRecord, 1, 33) = 'Rows inserted from staging table:', CONVERT(',', '', FIELD({{ ref('Runstream_TableName_ProsDate_RPT') }}.Record, ':', 2)), RowsInsertedFromStagingTable) AS RowsInsertedFromStagingTable,
		-- *SRC*: \(20)If TrimmedRecord[1, 33] = 'Total rows rejected:' Then Convert(',', '', Field(In_Record.Record, ':', 2)) Else TotalRowsRejected,
		IFF(SUBSTRING(TrimmedRecord, 1, 33) = 'Total rows rejected:', CONVERT(',', '', FIELD({{ ref('Runstream_TableName_ProsDate_RPT') }}.Record, ':', 2)), TotalRowsRejected) AS TotalRowsRejected,
		pODS_BATCH_ID AS BTCH_KEY_I,
		pcTABLE_NAME AS TRGT_M
	FROM {{ ref('Runstream_TableName_ProsDate_RPT') }}
	WHERE 
)

SELECT * FROM Find_MetaData