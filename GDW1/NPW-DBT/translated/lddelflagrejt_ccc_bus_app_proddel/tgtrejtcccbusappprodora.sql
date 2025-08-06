{{ config(materialized='view', tags=['LdDelFlagREJT_CCC_BUS_APP_PRODDel']) }}

SELECT
	CC_APP_PROD_ID 
FROM {{ ref('CpyRenameCols') }}