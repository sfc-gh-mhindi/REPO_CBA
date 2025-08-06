{{ config(materialized='view', tags=['LdMAP_APP_PROD_EXCL_ORAInsertFrmExt']) }}

SELECT
	APP_PROD_ID
	DUMMY_PDCT_F
	CRAT_D 
FROM {{ ref('SrcMapCseAppProdExclDS') }}