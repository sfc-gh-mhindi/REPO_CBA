{{ config(materialized='view', tags=['ExtComBusAppProdClientRole']) }}

WITH LkpExclusions AS (
	SELECT
		{{ ref('FunMergeJournal') }}.APP_PROD_CLIENT_ROLE_ID,
		{{ ref('FunMergeJournal') }}.ROLE_CAT_ID,
		{{ ref('FunMergeJournal') }}.CIF_CODE,
		{{ ref('FunMergeJournal') }}.APP_PROD_ID,
		{{ ref('FunMergeJournal') }}.SUBTYPE_CODE,
		{{ ref('FunMergeJournal') }}.ORIG_ETL_D,
		APP_PROD_CLIENT_ROLE_ID AS APP_PROD_CLIENT_ROLE_ID,
		ROLE_CAT_ID AS ROLE_CAT_ID,
		CIF_CODE AS CIF_CODE,
		APP_PROD_ID AS APP_PROD_ID,
		SUBTYPE_CODE AS SUBTYPE_CODE,
		ORIG_ETL_D AS ORIG_ETL_D
	FROM {{ ref('FunMergeJournal') }}
	LEFT JOIN {{ ref('SrcMapAPP_PROD_EXCLLks') }} ON 
)

SELECT * FROM LkpExclusions