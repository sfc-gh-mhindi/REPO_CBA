{{ config(materialized='view', tags=['XfmPL_APP_PRODFrmExt']) }}

WITH --Manual Task - ColumnGenerator - AddGrdColLkpKey

SELECT * FROM AddGrdColLkpKey