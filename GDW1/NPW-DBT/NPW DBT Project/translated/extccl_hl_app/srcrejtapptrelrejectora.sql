{{ config(materialized='view', tags=['ExtCCL_HL_APP']) }}

WITH 
rejt_ccl_hl_app AS (
	SELECT
	*
	FROM {{ source("cse4_stg","rejt_ccl_hl_app")  }}),
SrcRejtApptRelRejectOra AS (SELECT CCL_HL_APP_ID, CCL_APP_ID, HL_APP_ID, LMI_AMT, HL_PACKAGE_CAT_ID, CAST(ORIG_ETL_D AS TEXT) AS ORIG_ETL_D FROM REJT_CCL_HL_APP WHERE EROR_C LIKE 'RPR%')


SELECT * FROM SrcRejtApptRelRejectOra