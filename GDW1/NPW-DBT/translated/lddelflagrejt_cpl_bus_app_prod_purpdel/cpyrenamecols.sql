{{ config(materialized='view', tags=['LdDelFlagREJT_CPL_BUS_APP_PROD_PURPDel']) }}

WITH CpyRenameCols AS (
	SELECT
		{{ ref('PL_APP_PROD_PURPOSE') }}.DELETED_KEY_1_VALUE AS PL_APP_PROD_PURP_ID
	FROM {{ ref('PL_APP_PROD_PURPOSE') }}
)

SELECT * FROM CpyRenameCols