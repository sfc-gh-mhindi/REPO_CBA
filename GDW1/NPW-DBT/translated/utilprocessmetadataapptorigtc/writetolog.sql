{{ config(materialized='view', tags=['UtilProcessMetaDataApptOrigTC']) }}

WITH --Manual Task - None - WriteToLog

SELECT * FROM WriteToLog