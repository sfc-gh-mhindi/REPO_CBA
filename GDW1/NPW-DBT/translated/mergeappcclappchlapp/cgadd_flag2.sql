{{ config(materialized='view', tags=['MergeAppCclAppChlApp']) }}

WITH --Manual Task - ColumnGenerator - CgAdd_Flag2

SELECT * FROM CgAdd_Flag2