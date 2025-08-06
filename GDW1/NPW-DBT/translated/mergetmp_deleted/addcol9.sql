{{ config(materialized='view', tags=['MergeTMP_DELETED']) }}

WITH --Manual Task - ColumnGenerator - AddCol9

SELECT * FROM AddCol9