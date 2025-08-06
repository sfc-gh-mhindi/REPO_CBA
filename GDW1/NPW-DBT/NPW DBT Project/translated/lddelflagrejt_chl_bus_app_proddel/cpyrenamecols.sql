{{ config(materialized='view', tags=['LdDelFlagREJT_CHL_BUS_APP_PRODDel']) }}

WITH CpyRenameCols AS (
	SELECT
		{{ ref('HL_APP_PROD') }}.DELETED_KEY_1_VALUE AS HL_APP_PROD_ID
	FROM {{ ref('HL_APP_PROD') }}
)

SELECT * FROM CpyRenameCols