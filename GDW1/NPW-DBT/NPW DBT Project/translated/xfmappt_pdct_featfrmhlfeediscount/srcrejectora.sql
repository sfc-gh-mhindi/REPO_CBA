{{ config(materialized='view', tags=['XfmAPPT_PDCT_FEATFrmHlFeeDiscount']) }}

WITH 
rejt_chl_bus_fee_disc_fee AS (
	SELECT
	*
	FROM {{ source("cse4_stg","rejt_chl_bus_fee_disc_fee")  }}),
SrcRejectOra AS (SELECT HL_FEE_ID, BF_FOUND_FLAG, 'Y' AS REJT_FOUND_FLAG FROM REJT_CHL_BUS_FEE_DISC_FEE WHERE EROR_C LIKE 'RPR%')


SELECT * FROM SrcRejectOra