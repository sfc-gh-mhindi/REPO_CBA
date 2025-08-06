{{ config(materialized='view', tags=['MergeAppProdCclAppProdPlAppProd']) }}

WITH --Manual Task - ColumnGenerator - CgAdd_Flag2

SELECT * FROM CgAdd_Flag2