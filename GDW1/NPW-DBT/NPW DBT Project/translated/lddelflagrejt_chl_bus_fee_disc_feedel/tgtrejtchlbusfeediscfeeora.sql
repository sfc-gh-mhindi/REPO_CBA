{{ config(materialized='view', tags=['LdDelFlagREJT_CHL_BUS_FEE_DISC_FEEDel']) }}

SELECT
	HL_FEE_ID 
FROM {{ ref('FN_DeleteRecords') }}