{{ config(materialized='view', tags=['LdREJT_CSE_COI_BUS_PROP_CLNT']) }}

WITH ModConversions AS (
	SELECT 
	--Manual
	--ETL_D= date_from_string[%yyyy%mm%dd] (ETL_D)
	--ORIG_ETL_D= date_from_string[%yyyy%mm%dd] (ORIG_ETL_D)
	FA_PROPOSED_CLIENT_ID, COIN_ENTITY_ID, CLIENT_CORRELATION_ID, COIN_ENTITY_NAME, FA_ENTITY_CAT_ID, FA_UNDERTAKING_ID, FA_PROPOSED_CLIENT_CAT_ID, ETL_D, ORIG_ETL_D, EROR_C 
	FROM {{ ref('FunRejectsNulls') }}
)

SELECT * FROM ModConversions