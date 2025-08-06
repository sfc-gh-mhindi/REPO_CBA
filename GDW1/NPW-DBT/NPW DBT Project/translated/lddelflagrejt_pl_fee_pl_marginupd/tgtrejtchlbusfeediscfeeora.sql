{{ config(materialized='view', tags=['LdDelFlagREJT_PL_FEE_PL_MARGINUpd']) }}

SELECT
	PL_FEE_ID
	PL_MARGIN_MARGIN_AMT
	PL_MARGIN_MARGIN_REASON_CAT_ID
	PL_MARGIN_FOUND_FLAG
	ETL_D 
FROM {{ ref('ModConversions') }}