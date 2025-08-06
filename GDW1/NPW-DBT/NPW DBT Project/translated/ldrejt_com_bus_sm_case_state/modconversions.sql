{{ config(materialized='view', tags=['LdREJT_COM_BUS_SM_CASE_STATE']) }}

WITH ModConversions AS (
	SELECT 
	--Manual
	--ETL_D= date_from_string[%yyyy%mm%dd] (ETL_D)
	--ORIG_ETL_D= date_from_string[%yyyy%mm%dd] (ORIG_ETL_D)
	SM_CASE_STATE_ID, SM_CASE_ID, SM_STATE_CAT_ID, START_DATE, END_DATE, CREATED_BY_STAFF_NUMBER, STATE_CAUSED_BY_ACTION_ID, ETL_D, ORIG_ETL_D, EROR_C 
	FROM {{ ref('Funnel_93') }}
)

SELECT * FROM ModConversions