{{ config(materialized='view', tags=['GDWUtilProcessMetaDataFL']) }}

WITH Update_Util_Pros__ToRptFile AS (
	SELECT
		-- *SRC*: Update_Link.InputRows = Update_Link.RowsWritten and Update_Link.TotalRowsRejected = 0,
		{{ ref('Lookup') }}.InputRows = {{ ref('Lookup') }}.RowsWritten AND {{ ref('Lookup') }}.TotalRowsRejected = 0 AS svLoadOK,
		pcFILE_NAME AS FileName,
		InputRows,
		InputRowsRejected,
		DuplicateRows,
		RowsWritten,
		TotalRowsRejected
	FROM {{ ref('Lookup') }}
	WHERE 
)

SELECT * FROM Update_Util_Pros__ToRptFile