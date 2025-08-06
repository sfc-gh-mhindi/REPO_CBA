{{ config(materialized='view', tags=['XfmAPPT_PDCT_FEATFrmHlProdIntMargin']) }}

WITH 
rejt_rt_perc_prod_int_marg AS (
	SELECT
	*
	FROM {{ source("cse4_stg","rejt_rt_perc_prod_int_marg")  }}),
SrcRejectOra AS (SELECT HL_INT_RATE_ID, RATE_FOUND_FLAG, PERC_FOUND_FLAG, 'Y' AS REJT_FOUND_FLAG FROM REJT_RT_PERC_PROD_INT_MARG WHERE EROR_C LIKE 'RPR%')


SELECT * FROM SrcRejectOra