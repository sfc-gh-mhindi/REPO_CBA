{{ config(materialized='view', tags=['LdREJT_CPL_BUS_APP_PROD_PURP']) }}

WITH FunCplBusAppProdPurpNulls AS (
	SELECT
		PL_APP_PROD_PURP_ID as PL_APP_PROD_PURP_ID,
		PL_APP_PROD_PURP_CAT_ID as PL_APP_PROD_PURP_CAT_ID,
		AMT as AMT,
		PL_APP_PROD_ID as PL_APP_PROD_ID,
		ETL_D as ETL_D,
		ORIG_ETL_D as ORIG_ETL_D,
		EROR_C as EROR_C
	FROM {{ ref('RejCplBusAppProdPurpDS') }}
	UNION ALL
	SELECT
		PL_APP_PROD_PURP_ID,
		PL_APP_PROD_PURP_CAT_ID,
		AMT,
		PL_APP_PROD_ID,
		ETL_D,
		ORIG_ETL_D,
		EROR_C
	FROM {{ ref('SrcCplBusAppProdPurpRejectsDS') }}
)

SELECT * FROM FunCplBusAppProdPurpNulls