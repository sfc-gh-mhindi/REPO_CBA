{{ config(materialized='view', tags=['XfmPlFeePlMarginFrmExt']) }}

WITH --Manual Task - ColumnGenerator - CgAddMapTypeC

SELECT * FROM CgAddMapTypeC