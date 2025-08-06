{{ config(materialized='incremental', alias='_cba__app_ccods_uat_temp__co__mstr__20100824__rpt', incremental_strategy='insert_overwrite', tags=['GDWUtilProcessMetaDataFL']) }}

SELECT
	FileName,
	InputRows,
	InputRowsRejected,
	DuplicateRows,
	RowsWritten,
	TotalRowsRejected 
FROM {{ ref('Update_Util_Pros__ToRptFile') }}