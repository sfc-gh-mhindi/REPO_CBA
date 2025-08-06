{{ config(materialized='view', tags=['XfmHL_FEATURE_ATTRFrmExt']) }}

WITH --Manual Task - ColumnGenerator - AddDummyColForLkup2

SELECT * FROM AddDummyColForLkup2