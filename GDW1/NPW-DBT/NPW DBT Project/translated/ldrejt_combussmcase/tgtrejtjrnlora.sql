{{ config(materialized='view', tags=['LdREJT_ComBusSmCase']) }}

SELECT
	SM_CASE_ID
	CREATED_TIMESTAMP
	WIM_PROCESS_ID
	ETL_D
	ORIG_ETL_D
	EROR_C 
FROM {{ ref('ModConversions') }}