{{ config(materialized='view', tags=['UtilProcessMetaDataTC']) }}

WITH --Manual Task - None - WriteToLog

SELECT * FROM WriteToLog