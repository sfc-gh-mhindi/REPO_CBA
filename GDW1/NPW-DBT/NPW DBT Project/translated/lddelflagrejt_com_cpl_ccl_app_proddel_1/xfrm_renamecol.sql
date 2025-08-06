{{ config(materialized='view', tags=['LdDelFlagREJT_COM_CPL_CCL_APP_PRODDel_1']) }}

WITH Xfrm_RenameCol AS (
	SELECT
		{{ ref('CCL_APP_PROD') }}.DELETED_KEY_1_VALUE AS CCL_APP_PROD_ID,
		{{ ref('CCL_APP_PROD') }}.DELETED_KEY_1_VALUE AS COM_APP_PROD_ID
	FROM {{ ref('CCL_APP_PROD') }}
	WHERE 
)

SELECT * FROM Xfrm_RenameCol