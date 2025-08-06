{{ config(materialized='view', tags=['XfmAppt_Pdct_Chkl']) }}

WITH CpySrc AS (
	SELECT
		APP_PROD_ID,
		READ_COSTS_AND_RISKS_FLAG,
		ACCEPTS_COSTS_AND_RISKS_DATE
	FROM {{ ref('SrcCseCpoBusAppProd') }}
)

SELECT * FROM CpySrc