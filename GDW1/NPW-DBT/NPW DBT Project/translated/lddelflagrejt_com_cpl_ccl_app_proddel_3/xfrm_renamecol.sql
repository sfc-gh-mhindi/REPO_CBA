{{ config(materialized='view', tags=['LdDelFlagREJT_COM_CPL_CCL_APP_PRODDel_3']) }}

WITH Xfrm_RenameCol AS (
	SELECT
		{{ ref('PL_APP_PROD') }}.DELETED_KEY_1_VALUE AS CPL_APP_PROD_ID,
		{{ ref('PL_APP_PROD') }}.DELETED_KEY_1_VALUE AS COM_APP_PROD_ID
	FROM {{ ref('PL_APP_PROD') }}
	WHERE 
)

SELECT * FROM Xfrm_RenameCol