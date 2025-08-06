{{ config(materialized='view', tags=['MergeTMP_DELETED']) }}

WITH --Manual Task - ColumnGenerator - AddCol6

SELECT * FROM AddCol6