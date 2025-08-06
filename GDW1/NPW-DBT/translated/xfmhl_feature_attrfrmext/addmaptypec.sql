{{ config(materialized='view', tags=['XfmHL_FEATURE_ATTRFrmExt']) }}

WITH --Manual Task - ColumnGenerator - AddMapTypeC

SELECT * FROM AddMapTypeC