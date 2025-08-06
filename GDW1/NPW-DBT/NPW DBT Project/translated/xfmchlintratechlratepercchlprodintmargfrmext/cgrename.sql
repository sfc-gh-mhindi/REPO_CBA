{{ config(materialized='view', tags=['XfmChlIntRateChlRatePercChlProdIntMargFrmExt']) }}

WITH --Manual Task - ColumnGenerator - CgRename

SELECT * FROM CgRename