{{ config(materialized='view', tags=['LdDelFlagREJT_CCL_APP_FEE_FEE_TYPE_CATDel']) }}

SELECT
	CCL_APP_FEE_ID 
FROM {{ ref('CpyRenameCols') }}