{{ config(materialized='view', tags=['GDWUtilProcessMetaDataFL']) }}

WITH --Manual Task - Tail - Last_Record

SELECT * FROM Last_Record