{{ config(materialized='view', tags=['LdDelFlagREJT_COM_CPL_CCL_APP_PRODDel_2']) }}

WITH CpyRenameHlAppProdCols AS (
	SELECT
		{{ ref('HL_APP_PROD') }}.DELETED_KEY_1_VALUE AS COM_APP_PROD_ID
	FROM {{ ref('HL_APP_PROD') }}
)

SELECT * FROM CpyRenameHlAppProdCols