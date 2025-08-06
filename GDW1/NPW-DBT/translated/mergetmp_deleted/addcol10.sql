{{ config(materialized='view', tags=['MergeTMP_DELETED']) }}

WITH --Manual Task - ColumnGenerator - AddCol10

SELECT * FROM AddCol10