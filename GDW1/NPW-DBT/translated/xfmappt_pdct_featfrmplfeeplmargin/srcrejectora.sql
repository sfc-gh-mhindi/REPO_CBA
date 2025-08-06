{{ config(materialized='view', tags=['XfmAPPT_PDCT_FEATFrmPlFeePlMargin']) }}

WITH 
rejt_pl_fee_pl_margin AS (
	SELECT
	*
	FROM {{ source("cse4_stg","rejt_pl_fee_pl_margin")  }}),
SrcRejectOra AS (SELECT PL_FEE_ID, PL_FEE_FOUND_FLAG, 'Y' AS REJT_FOUND_FLAG FROM REJT_PL_FEE_PL_MARGIN WHERE EROR_C LIKE 'RPR%')


SELECT * FROM SrcRejectOra