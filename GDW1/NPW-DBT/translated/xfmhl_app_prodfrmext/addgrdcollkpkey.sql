{{ config(materialized='view', tags=['XfmHL_APP_PRODFrmExt']) }}

WITH --Manual Task - ColumnGenerator - AddGrdColLkpKey

SELECT * FROM AddGrdColLkpKey