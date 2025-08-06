{{ config(materialized='view', tags=['ExtPL_APP']) }}

WITH 
rejt_cpl_bus_app AS (
	SELECT
	*
	FROM {{ source("cse4_stg","rejt_cpl_bus_app")  }}),
SrcRejtPlAppRejectOra AS (SELECT PL_APP_ID, NOMINATED_BRANCH_ID, PL_PACKAGE_CAT_ID, CAST(ORIG_ETL_D AS TEXT) AS ORIG_ETL_D FROM REJT_CPL_BUS_APP WHERE EROR_C LIKE 'RPR%')


SELECT * FROM SrcRejtPlAppRejectOra