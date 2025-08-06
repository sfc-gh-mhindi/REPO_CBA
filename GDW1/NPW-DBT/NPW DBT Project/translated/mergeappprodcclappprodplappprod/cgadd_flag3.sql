{{ config(materialized='view', tags=['MergeAppProdCclAppProdPlAppProd']) }}

WITH --Manual Task - ColumnGenerator - CgAdd_Flag3

SELECT * FROM CgAdd_Flag3