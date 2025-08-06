{{ config(materialized='view', tags=['LdDelFlagREJT_CHL_BUS_APP_PROD_PURPDel']) }}

WITH CpyRenameCols AS (
	SELECT
		{{ ref('HL_APP_PROD_PURPOSE') }}.DELETED_KEY_1_VALUE AS HL_APP_PROD_PURPOSE_ID
	FROM {{ ref('HL_APP_PROD_PURPOSE') }}
)

SELECT * FROM CpyRenameCols