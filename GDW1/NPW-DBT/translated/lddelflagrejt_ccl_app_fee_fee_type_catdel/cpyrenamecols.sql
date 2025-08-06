{{ config(materialized='view', tags=['LdDelFlagREJT_CCL_APP_FEE_FEE_TYPE_CATDel']) }}

WITH CpyRenameCols AS (
	SELECT
		{{ ref('CCL_APP_FEE') }}.DELETED_KEY_1_VALUE AS CCL_APP_FEE_ID
	FROM {{ ref('CCL_APP_FEE') }}
)

SELECT * FROM CpyRenameCols