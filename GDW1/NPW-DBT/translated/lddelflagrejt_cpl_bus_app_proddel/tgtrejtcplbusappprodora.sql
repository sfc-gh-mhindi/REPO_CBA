{{ config(materialized='view', tags=['LdDelFlagREJT_CPL_BUS_APP_PRODDel']) }}

SELECT
	PL_APP_PROD_ID 
FROM {{ ref('CpyRenameCols') }}