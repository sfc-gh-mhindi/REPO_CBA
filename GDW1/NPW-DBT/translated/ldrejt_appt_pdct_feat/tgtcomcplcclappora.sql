{{ config(materialized='view', tags=['LdREJT_APPT_PDCT_FEAT']) }}

SELECT
	APP_PROD_ID
	COM_SUBTYPE_CODE
	CAMPAIGN_CAT_ID
	COM_APP_ID
	ETL_D
	ORIG_ETL_D
	EROR_C 
FROM {{ ref('ModConversions') }}