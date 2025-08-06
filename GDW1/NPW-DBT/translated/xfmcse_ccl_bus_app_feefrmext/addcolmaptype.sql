{{ config(materialized='view', tags=['XfmCSE_CCL_BUS_APP_FEEFrmExt']) }}

WITH --Manual Task - ColumnGenerator - AddColMapType

SELECT * FROM AddColMapType