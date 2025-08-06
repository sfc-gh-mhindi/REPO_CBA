{{ config(materialized='view', tags=['LdREJT_CSE_CPL_BUS_APP']) }}

WITH FunCseCplBusNulls AS (
	SELECT
		PL_APP_ID as PL_APP_ID,
		NOMINATED_BRANCH_ID as NOMINATED_BRANCH_ID,
		PL_PACKAGE_CAT_ID as PL_PACKAGE_CAT_ID,
		ETL_D as ETL_D,
		ORIG_ETL_D as ORIG_ETL_D,
		EROR_C as EROR_C
	FROM {{ ref('RejCseCplBusDS') }}
	UNION ALL
	SELECT
		PL_APP_ID,
		NOMINATED_BRANCH_ID,
		PL_PACKAGE_CAT_ID,
		ETL_D,
		ORIG_ETL_D,
		EROR_C
	FROM {{ ref('SrcCseCplBusRejectsDS') }}
)

SELECT * FROM FunCseCplBusNulls