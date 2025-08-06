{{ config(materialized='view', tags=['XfmCC_APP_PRODFrmExt']) }}

WITH --Manual Task - ColumnGenerator - CgAddMapTypeC

SELECT * FROM CgAddMapTypeC