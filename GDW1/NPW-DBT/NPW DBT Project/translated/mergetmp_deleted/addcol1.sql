{{ config(materialized='view', tags=['MergeTMP_DELETED']) }}

WITH --Manual Task - ColumnGenerator - AddCol1

SELECT * FROM AddCol1