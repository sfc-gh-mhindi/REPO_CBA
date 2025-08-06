{{ config(materialized='view', tags=['LdDelFlagREJT_CCL_APP_PRODDel']) }}

WITH RenameCol AS (
	SELECT
		{{ ref('CCL_APP_PROD') }}.DELETED_KEY_1_VALUE AS CCL_APP_PROD_ID
	FROM {{ ref('CCL_APP_PROD') }}
)

SELECT * FROM RenameCol