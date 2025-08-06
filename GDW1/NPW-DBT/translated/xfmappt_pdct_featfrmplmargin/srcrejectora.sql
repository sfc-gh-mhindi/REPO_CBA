{{ config(materialized='view', tags=['XfmAPPT_PDCT_FEATFrmPlMargin']) }}

WITH 
rejt_cpl_bus_int_rate_amt_marg AS (
	SELECT
	*
	FROM {{ source("cse4_stg","rejt_cpl_bus_int_rate_amt_marg")  }}),
SrcRejectOra AS (SELECT PL_INT_RATE_ID, PL_INT_RATE_FOUND_FLAG, PL_INT_RATE_AMT_FOUND_FLAG, 'Y' AS REJT_FOUND_FLAG FROM REJT_CPL_BUS_INT_RATE_AMT_MARG WHERE EROR_C LIKE 'RPR%')


SELECT * FROM SrcRejectOra