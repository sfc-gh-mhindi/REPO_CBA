{{ config(materialized='view', tags=['MergeBusHlmAppTpb']) }}

WITH --Manual Task - ColumnGenerator - CgAdd_Flag1

SELECT * FROM CgAdd_Flag1