{{ config(materialized='view', tags=['XfmCclAppProdFrmExt2']) }}

WITH --Manual Task - ColumnGenerator - CgSetLiterals

SELECT * FROM CgSetLiterals