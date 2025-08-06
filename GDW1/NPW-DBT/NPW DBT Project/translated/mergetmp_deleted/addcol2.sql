{{ config(materialized='view', tags=['MergeTMP_DELETED']) }}

WITH --Manual Task - ColumnGenerator - AddCol2

SELECT * FROM AddCol2