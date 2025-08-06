{{ config(materialized='view', tags=['MergeAppProdCclAppProdPlAppProd']) }}

WITH --Manual Task - ColumnGenerator - CgAdd_Flag5

SELECT * FROM CgAdd_Flag5