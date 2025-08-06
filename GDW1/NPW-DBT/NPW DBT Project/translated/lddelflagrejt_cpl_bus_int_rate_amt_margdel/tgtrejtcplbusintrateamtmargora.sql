{{ config(materialized='view', tags=['LdDelFlagREJT_CPL_BUS_INT_RATE_AMT_MARGDel']) }}

SELECT
	PL_INT_RATE_ID 
FROM {{ ref('FN_DeleteRecords') }}