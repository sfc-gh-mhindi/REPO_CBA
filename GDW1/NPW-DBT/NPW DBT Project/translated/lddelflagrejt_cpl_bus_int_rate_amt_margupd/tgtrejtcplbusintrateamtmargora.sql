{{ config(materialized='view', tags=['LdDelFlagREJT_CPL_BUS_INT_RATE_AMT_MARGUpd']) }}

SELECT
	PL_INT_RATE_ID
	PL_MARGIN_MARGIN_AMT
	PL_MARGIN_MARGIN_RESN_CAT_ID
	PL_MARGIN_FOUND_FLAG
	ETL_D 
FROM {{ ref('ModConversions') }}