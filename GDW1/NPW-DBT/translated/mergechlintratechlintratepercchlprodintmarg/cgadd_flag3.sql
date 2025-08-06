{{ config(materialized='view', tags=['MergeChlIntRateChlIntRatePercChlProdIntMarg']) }}

WITH --Manual Task - ColumnGenerator - CgAdd_Flag3

SELECT * FROM CgAdd_Flag3