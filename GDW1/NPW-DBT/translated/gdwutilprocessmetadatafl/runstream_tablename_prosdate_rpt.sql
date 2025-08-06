{{ config(materialized='view', tags=['GDWUtilProcessMetaDataFL']) }}

WITH 
_cba__app_ccods_uat_temp______var__dbt__pcfile__name______co__mstr__20100824 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_ccods_uat_temp______var__dbt__pcfile__name______co__mstr__20100824")  }})
Runstream_TableName_ProsDate_RPT AS (
	SELECT Record
	FROM _cba__app_ccods_uat_temp______var__dbt__pcfile__name______co__mstr__20100824
)

SELECT * FROM Runstream_TableName_ProsDate_RPT