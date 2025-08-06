{{ config(materialized='view', tags=['MergeChlBusFeeDisc']) }}

WITH --Manual Task - ColumnGenerator - CgAdd_Flag2

SELECT * FROM CgAdd_Flag2