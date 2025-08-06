{{ config(materialized='view', tags=['REJT_COI_BUS_CLNT_UNDTAK']) }}

WITH ModConversions AS (
	SELECT 
	--Manual
	--ETL_D= date_from_string[%yyyy%mm%dd] (ETL_D)
	--ORIG_ETL_D= date_from_string[%yyyy%mm%dd] (ORIG_ETL_D)
	FA_CLIENT_UNDERTAKING_ID, FA_UNDERTAKING_ID, COIN_ENTITY_ID, CLIENT_CORRELATION_ID, FA_ENTITY_CAT_ID, FA_CHILD_STATUS_CAT_ID, CLIENT_RELATIONSHIP_TYPE_ID, CLIENT_POSITION, IS_PRIMARY_FLAG, CIF_CODE, ETL_D, ORIG_ETL_D, EROR_C 
	FROM {{ ref('FunRejectsNulls') }}
)

SELECT * FROM ModConversions