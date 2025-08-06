{{ config(materialized='view', tags=['LdREJT_ComBusSmCase']) }}

WITH ModConversions AS (
	SELECT 
	--Manual
	--ETL_D= date_from_string[%yyyy%mm%dd] (ETL_D)
	--ORIG_ETL_D= date_from_string[%yyyy%mm%dd] (ORIG_ETL_D)
	SM_CASE_ID, CREATED_TIMESTAMP, WIM_PROCESS_ID, ETL_D, ORIG_ETL_D, EROR_C 
	FROM {{ ref('SrcRejJnlIdNullsDS') }}
)

SELECT * FROM ModConversions