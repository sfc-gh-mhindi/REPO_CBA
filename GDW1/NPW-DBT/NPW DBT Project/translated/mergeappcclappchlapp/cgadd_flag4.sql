{{ config(materialized='view', tags=['MergeAppCclAppChlApp']) }}

WITH --Manual Task - ColumnGenerator - CgAdd_Flag4

SELECT * FROM CgAdd_Flag4