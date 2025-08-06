{{ config(materialized='view', tags=['LdDelFlagREJT_CPL_BUS_APP_PROD_PURPDel']) }}

SELECT
	PL_APP_PROD_PURP_ID 
FROM {{ ref('CpyRenameCols') }}