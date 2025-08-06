{{ config(materialized='view', tags=['UtilProcessMetaDataTC_Promo']) }}

WITH --Manual Task - None - WriteToLog

SELECT * FROM WriteToLog