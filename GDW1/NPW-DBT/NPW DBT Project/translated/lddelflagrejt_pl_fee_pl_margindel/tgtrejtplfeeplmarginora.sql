{{ config(materialized='view', tags=['LdDelFlagREJT_PL_FEE_PL_MARGINDel']) }}

SELECT
	PL_FEE_ID 
FROM {{ ref('FN_DeleteRecords') }}