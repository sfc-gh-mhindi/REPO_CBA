{{ config(materialized='view', tags=['LdDelFlagREJT_CHL_BUS_APP_PRODDel']) }}

SELECT
	HL_APP_PROD_ID 
FROM {{ ref('CpyRenameCols') }}