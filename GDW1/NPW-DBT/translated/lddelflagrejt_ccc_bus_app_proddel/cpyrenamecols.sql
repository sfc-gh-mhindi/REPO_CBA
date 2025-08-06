{{ config(materialized='view', tags=['LdDelFlagREJT_CCC_BUS_APP_PRODDel']) }}

WITH CpyRenameCols AS (
	SELECT
		{{ ref('CC_APP_PROD') }}.DELETED_KEY_1_VALUE AS CC_APP_PROD_ID
	FROM {{ ref('CC_APP_PROD') }}
)

SELECT * FROM CpyRenameCols