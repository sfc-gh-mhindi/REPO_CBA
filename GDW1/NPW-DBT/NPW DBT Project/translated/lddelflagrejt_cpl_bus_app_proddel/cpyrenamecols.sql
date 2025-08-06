{{ config(materialized='view', tags=['LdDelFlagREJT_CPL_BUS_APP_PRODDel']) }}

WITH CpyRenameCols AS (
	SELECT
		{{ ref('PL_APP_PROD') }}.DELETED_KEY_1_VALUE AS PL_APP_PROD_ID
	FROM {{ ref('PL_APP_PROD') }}
)

SELECT * FROM CpyRenameCols