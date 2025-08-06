{{ config(materialized='view', tags=['LdDelFlagREJT_CCC_BUS_APP_PROD_BAL_XFERDel']) }}

SELECT
	CC_APP_PROD_BAL_XFER_ID 
FROM {{ ref('CpyRenameCols') }}