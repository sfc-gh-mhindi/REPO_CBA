{{ config(materialized='view', tags=['LdDelFlagREJT_CCC_BUS_APP_PROD_BAL_XFERDel']) }}

WITH CpyRenameCols AS (
	SELECT
		{{ ref('CC_APP_PROD_BAL_XFER') }}.DELETED_KEY_1_VALUE AS CC_APP_PROD_BAL_XFER_ID
	FROM {{ ref('CC_APP_PROD_BAL_XFER') }}
)

SELECT * FROM CpyRenameCols