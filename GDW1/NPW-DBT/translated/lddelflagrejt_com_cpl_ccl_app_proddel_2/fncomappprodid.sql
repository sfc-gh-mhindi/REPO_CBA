{{ config(materialized='view', tags=['LdDelFlagREJT_COM_CPL_CCL_APP_PRODDel_2']) }}

WITH FnComAppProdId AS (
	SELECT
		COM_APP_PROD_ID as COM_APP_PROD_ID
	FROM {{ ref('CpyRenameHlAppProdCols') }}
	UNION ALL
	SELECT
		COM_APP_PROD_ID
	FROM {{ ref('CpyRenameCcAppProdCols') }}
)

SELECT * FROM FnComAppProdId