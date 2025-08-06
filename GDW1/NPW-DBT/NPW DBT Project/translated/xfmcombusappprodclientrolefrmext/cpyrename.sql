{{ config(materialized='view', tags=['XfmComBusAppProdClientRoleFrmExt']) }}

WITH CpyRename AS (
	SELECT
		APP_PROD_CLIENT_ROLE_ID,
		ROLE_CAT_ID,
		CIF_CODE,
		APP_PROD_ID,
		{{ ref('SrcBusApPrdClntPremapDS') }}.SUBTYPE_CODE AS SBTY_CODE,
		ORIG_ETL_D
	FROM {{ ref('SrcBusApPrdClntPremapDS') }}
)

SELECT * FROM CpyRename