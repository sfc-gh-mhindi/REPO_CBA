{{ config(materialized='view', tags=['LdDelFlagREJT_CHL_BUS_APP_PROD_PURPDel']) }}

SELECT
	HL_APP_PROD_PURPOSE_ID 
FROM {{ ref('CpyRenameCols') }}