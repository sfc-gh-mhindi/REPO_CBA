{{ config(materialized='view', tags=['XfmAppt_Pdct']) }}

WITH FiltrNullAndNotNull AS (
	SELECT
		APP_PROD_ID,
		APP_ID,
		PDCT_N,
		PO_OVERDRAFT_CAT_ID,
		APP_PROD_ID,
		APP_ID,
		PDCT_N,
		PO_OVERDRAFT_CAT_ID,
		APPT_PDCT_CATG_C,
		APPT_PDCT_DURT_C
	FROM {{ ref('Trans') }}
)

SELECT * FROM FiltrNullAndNotNull