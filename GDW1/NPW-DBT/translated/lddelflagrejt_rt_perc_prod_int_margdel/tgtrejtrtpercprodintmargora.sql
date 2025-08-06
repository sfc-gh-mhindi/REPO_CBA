{{ config(materialized='view', tags=['LdDelFlagREJT_RT_PERC_PROD_INT_MARGDel']) }}

SELECT
	HL_INT_RATE_ID 
FROM {{ ref('FN_DeleteRecords') }}