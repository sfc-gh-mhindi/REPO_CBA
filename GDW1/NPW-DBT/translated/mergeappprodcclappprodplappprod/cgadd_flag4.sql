{{ config(materialized='view', tags=['MergeAppProdCclAppProdPlAppProd']) }}

WITH --Manual Task - ColumnGenerator - CgAdd_Flag4

SELECT * FROM CgAdd_Flag4