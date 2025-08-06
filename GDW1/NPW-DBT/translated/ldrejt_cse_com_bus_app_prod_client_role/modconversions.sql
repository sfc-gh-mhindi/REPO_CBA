{{ config(materialized='view', tags=['LdREJT_CSE_COM_BUS_APP_PROD_CLIENT_ROLE']) }}

WITH ModConversions AS (
	SELECT 
	--Manual
	--ETL_D= date_from_string[%yyyy%mm%dd] (ETL_D)
	--ORIG_ETL_D= date_from_string[%yyyy%mm%dd] (ORIG_ETL_D)
	APP_PROD_CLIENT_ROLE_ID, ROLE_CAT_ID, CIF_CODE, APP_PROD_ID, SUBTYPE_CODE, ETL_D, ORIG_ETL_D, EROR_C 
	FROM {{ ref('FunJnlIdNulls') }}
)

SELECT * FROM ModConversions