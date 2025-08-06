{{ config(materialized='view', tags=['LdREJT_APPT_PDCT_FEAT']) }}

WITH FunRejectsNulls AS (
	SELECT
		APP_PROD_ID as APP_PROD_ID,
		COM_SUBTYPE_CODE as COM_SUBTYPE_CODE,
		CAMPAIGN_CAT_ID as CAMPAIGN_CAT_ID,
		COM_APP_ID as COM_APP_ID,
		ETL_D as ETL_D,
		ORIG_ETL_D as ORIG_ETL_D,
		EROR_C as EROR_C
	FROM {{ ref('SrcIdNullsDS') }}
	UNION ALL
	SELECT
		APP_PROD_ID,
		COM_SUBTYPE_CODE,
		CAMPAIGN_CAT_ID,
		COM_APP_ID,
		ETL_D,
		ORIG_ETL_D,
		EROR_C
	FROM {{ ref('SrcAppCclAppRejectsDS') }}
)

SELECT * FROM FunRejectsNulls